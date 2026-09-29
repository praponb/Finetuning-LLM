from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_warehouse_opened_on = Window.orderBy("warehouse_opened_on", "id")
w_all_by_qty_distance = Window.orderBy(F.col("qty_distance").desc(), "id")
w_warehouse_city = Window.partitionBy("warehouse_city")
w_move_type_len_warehouse_open = Window.partitionBy("move_type_len", F.year("warehouse_opened_on")).orderBy(F.col("qty_distance").desc(), "id")
w_all_by_move_type_len_moving = Window.orderBy("move_type_len", "id").rowsBetween(-1, Window.currentRow)
w_all_by_move_type_len = Window.orderBy("move_type_len", "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_id_distinct_sku_id_in_wa = Window.partitionBy("sku_id", "distinct_sku_id_in_warehouse_id").orderBy("warehouse_city_max_warehouse_capacity", "id")
w_sku_id_distinct_sku_id_in_wa_2 = Window.partitionBy("sku_id", "distinct_sku_id_in_warehouse_id")
w_distinct_operator_in_warehou = Window.partitionBy("distinct_operator_in_warehouse_city").orderBy("warehouse_id_min_qty_distance", "id")
w_sku_id_distinct_sku_id_in_wa_3 = Window.partitionBy("sku_id", "distinct_sku_id_in_warehouse_id").orderBy("warehouse_city_max_warehouse_capacity", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_moves = (
    df_stock_moves
    .filter((F.col("qty") >= 138) & (F.to_date(F.col("moved_at")) < F.lit("2025-02-24").cast("date")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.abs(F.col("qty") - 275).alias("qty_distance"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

combined_details_warehouse = (
    valid_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("sk.labels").alias("sku_labels"),
        "sk.sku_code",
    )
)

with_trends = (
    combined_details_warehouse
    .select(
        "*",
        F.ntile(5).over(w_all_by_warehouse_opened_on).alias("warehouse_opened_on_quintile"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_qty_distance)).alias("unit_price_change"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_city), 2).alias("unit_price_vs_warehouse_city_avg"),
        F.ntile(10).over(w_move_type_len_warehouse_open).alias("qty_distance_decile"),
        F.max("warehouse_capacity").over(w_warehouse_city).alias("warehouse_city_max_warehouse_capacity"),
        F.count(F.lit(1)).over(w_warehouse_city).alias("rows_in_warehouse_city"),
        F.round(F.avg("move_type_len").over(w_all_by_move_type_len_moving), 2).alias("move_type_len_avg_2"),
        F.datediff(F.col("warehouse_opened_on"), F.lag("warehouse_opened_on").over(w_all_by_move_type_len)).alias("days_since_prev_warehouse_opened_on"),
        F.size(F.collect_set("operator").over(w_warehouse_city)).alias("distinct_operator_in_warehouse_city"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id), 2).alias("unit_price_vs_warehouse_id_avg"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_warehouse_id), F.lit(0)), 4).alias("unit_price_share_of_warehouse_id"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id)).alias("distinct_sku_id_in_warehouse_id"),
        F.min("qty_distance").over(w_warehouse_id).alias("warehouse_id_min_qty_distance"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "*",
        F.lead("qty_distance_decile").over(w_sku_id_distinct_sku_id_in_wa).alias("next_qty_distance_decile"),
        F.min("unit_price_change").over(w_sku_id_distinct_sku_id_in_wa_2).alias("sku_id_distinct_sku_id_in_warehouse_id_min_unit_"),
        F.round(F.percent_rank().over(w_distinct_operator_in_warehou), 3).alias("warehouse_id_min_qty_distance_percent_rank"),
        F.nth_value("warehouse_opened_on_quintile", 2).over(w_sku_id_distinct_sku_id_in_wa_3).alias("second_warehouse_opened_on_quintile"),
        F.sum("warehouse_capacity").over(w_sku_id_distinct_sku_id_in_wa_2).alias("sku_id_distinct_sku_id_in_warehouse_id_sum_wareh"),
        F.count(F.lit(1)).over(w_sku_id_distinct_sku_id_in_wa_2).alias("rows_in_sku_id_distinct_sku_id_in_warehouse_id"),
        F.min("qty_distance_decile").over(w_sku_id_distinct_sku_id_in_wa_2).alias("sku_id_distinct_sku_id_in_warehouse_id_min_qty_d"),
        F.ntile(10).over(w_sku_id_distinct_sku_id_in_wa).alias("warehouse_city_max_warehouse_capacity_decile"),
    )
)

rollup_base = (
    with_trends_2
    .filter(F.col("sku_id_distinct_sku_id_in_warehouse_id_min_unit_") <= 2800.0)
    .groupBy(
        "qty_distance_decile",
        F.date_format("moved_at", "yyyy-MM").alias("moved_at_ym"),
    )
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.round(F.sum("unit_price_change") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("unit_price_change_to_qty_ratio"),
        F.sum(F.when(F.col("warehouse_city").isin("Kuala Lumpur", "Bangkok"), 1).otherwise(0)).alias("n_warehouse_city_in_kuala_lump"),
        F.count("*").alias("n_rows"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.max("move_type_len_avg_2").alias("max_move_type_len_avg_2"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    rollup_base
    .select(
        "qty_distance_decile",
        "total_unit_price",
        "unit_price_change_to_qty_ratio",
        "n_warehouse_city_in_kuala_lump",
        "n_rows",
        "max_move_type_len_avg_2",
        F.length(F.trim(F.col("moved_at_ym"))).alias("moved_at_ym_len"),
    )
    .orderBy(F.col("total_unit_price").desc())
)
