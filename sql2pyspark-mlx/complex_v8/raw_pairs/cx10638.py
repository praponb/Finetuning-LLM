from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id_by_wareh = Window.partitionBy("warehouse_id", "sku_id").orderBy("warehouse_opened_on", "id")
w_warehouse_city = Window.partitionBy("warehouse_city")
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_all_by_qty = Window.orderBy("qty", "id")
w_move_type_sku_id_by_qty = Window.partitionBy("move_type", "sku_id").orderBy(F.col("qty").desc(), "id")
w_operator_by_unit_price_ties = Window.partitionBy("operator").orderBy(F.col("unit_price").desc())
w_operator_by_unit_price_runni = Window.partitionBy("operator").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_sku_id_by_qty_full = Window.partitionBy("move_type", "sku_id").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_operator_by_moved_at = Window.partitionBy("sku_id", "operator").orderBy(F.col("moved_at").desc_nulls_first(), "id")
w_distinct_operators_n_warehou = Window.partitionBy("distinct_operators", "n_warehouse_wh_code_is_not_nul").orderBy("distinct_operators")
w_cnt_distinct_operators = Window.partitionBy("cnt", "distinct_operators")
w_distinct_operators_n_warehou_2 = Window.partitionBy("distinct_operators", "n_warehouse_wh_code_is_not_nul").orderBy("distinct_operators", "warehouse_city").rowsBetween(-3, -1)

cleaned_moves = (
    df_stock_moves
    .filter((F.col("operator") == "op_eve") & (~F.col("move_type").isin("OUTBOUND", "INBOUND")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

joined = (
    cleaned_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.*",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

windowed = (
    joined
    .select(
        "*",
        F.lead("qty", 3).over(w_warehouse_id_sku_id_by_wareh).alias("next3_qty"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_warehouse_city), F.lit(0)), 4).alias("unit_price_share_of_warehouse_city"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id_sku_id), 2).alias("unit_price_vs_warehouse_id_sku_id_avg"),
        F.datediff(F.col("warehouse_opened_on"), F.lag("warehouse_opened_on").over(w_all_by_qty)).alias("days_since_prev_warehouse_opened_on"),
        F.dense_rank().over(w_move_type_sku_id_by_qty).alias("qty_dense_rank"),
        F.min("unit_price").over(w_warehouse_id_sku_id).alias("warehouse_id_sku_id_min_unit_price"),
        F.rank().over(w_operator_by_unit_price_ties).alias("unit_price_rank"),
        F.lag("unit_price").over(w_all_by_qty).alias("prev_unit_price"),
        F.sum("unit_price").over(w_operator_by_unit_price_runni).alias("running_unit_price"),
        F.count(F.lit(1)).over(w_warehouse_city).alias("rows_in_warehouse_city"),
        F.count(F.lit(1)).over(w_warehouse_id_sku_id).alias("rows_in_warehouse_id_sku_id"),
        F.last("qty").over(w_move_type_sku_id_by_qty_full).alias("last_qty"),
        F.size(F.collect_set("operator").over(w_warehouse_city)).alias("distinct_operator_in_warehouse_city"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_qty)).alias("unit_price_change"),
        F.size(F.collect_set("warehouse_id").over(w_warehouse_city)).alias("distinct_warehouse_id_in_warehouse_city"),
        F.lag("warehouse_opened_on").over(w_sku_id_operator_by_moved_at).alias("prev_warehouse_opened_on"),
    )
)

top_rows = windowed.filter(F.col("qty_dense_rank") == 1)

summary = (
    top_rows
    .filter(F.col("rows_in_warehouse_id_sku_id") > 4)
    .groupBy("warehouse_city")
    .agg(
        F.max("qty").alias("max_qty"),
        F.count(F.when(F.col("warehouse_wh_code").isNotNull(), 1)).alias("n_warehouse_wh_code_is_not_nul"),
        F.array_sort(F.collect_set("warehouse_city")).alias("warehouse_city_list"),
        F.min("unit_price_share_of_warehouse_city").alias("min_unit_price_share_of_warehouse_city"),
        F.count("*").alias("cnt"),
        F.countDistinct("operator").alias("distinct_operators"),
        F.sum("unit_price_rank").alias("sum_unit_price_rank_check"),
    )
    .filter(F.col("sum_unit_price_rank_check") > 5)
    .drop("sum_unit_price_rank_check")
)

sequenced = (
    summary
    .select(
        "*",
        F.dense_rank().over(w_distinct_operators_n_warehou).alias("distinct_operators_dense_rank"),
        F.max("max_qty").over(w_cnt_distinct_operators).alias("cnt_distinct_operators_max_max_qty"),
        F.round(F.avg("distinct_operators").over(w_distinct_operators_n_warehou_2), 2).alias("distinct_operators_avg_3_prior"),
    )
)

result = (
    sequenced
    .filter(F.col("cnt_distinct_operators_max_max_qty") <= 3250)
    .select(
        "warehouse_city",
        "max_qty",
        "n_warehouse_wh_code_is_not_nul",
        "warehouse_city_list",
        "distinct_operators",
        "distinct_operators_avg_3_prior",
    )
)
