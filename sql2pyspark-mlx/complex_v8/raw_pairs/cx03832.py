from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_all_by_unit_price_running = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_price_moving_6 = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(-6, Window.currentRow)
w_sku_is_active_operator = Window.partitionBy("sku_is_active", "operator")
w_all_by_unit_price_2 = Window.orderBy(F.col("unit_price").desc(), "id")
w_warehouse_id_by_unit_price = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").asc_nulls_last(), "id")
w_all_by_unit_price_prior_3 = Window.orderBy("unit_price", "id").rowsBetween(-3, -1)
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_all_by_unit_price_ties = Window.orderBy(F.col("unit_price").desc())
w_warehouse_id_by_unit_price_r = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_unit_price_perce = Window.partitionBy("operator").orderBy("unit_price_percent_rank_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_unit_price_perce_2 = Window.partitionBy("operator").orderBy("unit_price_percent_rank_2", "id")
w_sku_is_active_by_next_unit_p = Window.partitionBy("sku_is_active").orderBy("next_unit_price", "id")
w_unit_price_rank_distinct_ope = Window.partitionBy("unit_price_rank", "distinct_operator_in_warehouse_id_sku_id")
w_distinct_warehouse_id_in_sku = Window.partitionBy("distinct_warehouse_id_in_sku_is_active_operator", "unit_price_seq").orderBy(F.col("rows_in_warehouse_id_sku_id").desc(), "id").rowsBetween(-3, -1)

cleaned_moves = (
    df_stock_moves
    .filter((F.col("sku_id") >= 3) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_parent = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price",
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

sequenced = (
    with_parent
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator",
        "sku_is_active",
        F.round(F.percent_rank().over(w_all_by_unit_price), 3).alias("unit_price_percent_rank"),
        F.sum("unit_price").over(w_all_by_unit_price_running).alias("running_unit_price"),
        F.round(F.avg("unit_price").over(w_all_by_unit_price_moving_6), 2).alias("unit_price_avg_7"),
        F.size(F.collect_set("warehouse_id").over(w_sku_is_active_operator)).alias("distinct_warehouse_id_in_sku_is_active_operator"),
        F.lead("unit_price", 1, 0).over(w_all_by_unit_price_2).alias("next_unit_price"),
        F.row_number().over(w_warehouse_id_by_unit_price).alias("unit_price_seq"),
        F.sum("unit_price").over(w_all_by_unit_price_prior_3).alias("unit_price_sum_3_prior"),
        F.size(F.collect_set("operator").over(w_warehouse_id_sku_id)).alias("distinct_operator_in_warehouse_id_sku_id"),
        F.round(F.percent_rank().over(w_all_by_unit_price_ties), 3).alias("unit_price_percent_rank_2"),
        F.count(F.lit(1)).over(w_warehouse_id_by_unit_price_r).alias("running_count"),
        F.rank().over(w_all_by_unit_price_ties).alias("unit_price_rank"),
        F.round(F.avg("unit_price").over(w_warehouse_id_sku_id), 2).alias("warehouse_id_sku_id_avg_unit_price"),
        F.count(F.lit(1)).over(w_sku_is_active_operator).alias("rows_in_sku_is_active_operator"),
        F.count(F.lit(1)).over(w_warehouse_id_sku_id).alias("rows_in_warehouse_id_sku_id"),
    )
)

windowed = (
    sequenced
    .select(
        "*",
        F.min("unit_price_percent_rank_2").over(w_operator_by_unit_price_perce).alias("min_unit_price_percent_rank_2_so_far"),
        F.rank().over(w_operator_by_unit_price_perce_2).alias("unit_price_percent_rank_2_rank"),
        F.lead("unit_price_sum_3_prior", 2).over(w_sku_is_active_by_next_unit_p).alias("next2_unit_price_sum_3_prior"),
        F.first("unit_price_seq", ignorenulls=True).over(w_operator_by_unit_price_perce).alias("first_known_unit_price_seq"),
        F.round(F.avg("unit_price_avg_7").over(w_unit_price_rank_distinct_ope), 2).alias("unit_price_rank_distinct_operator_in_warehouse_i"),
        F.round(F.avg("rows_in_warehouse_id_sku_id").over(w_distinct_warehouse_id_in_sku), 2).alias("rows_in_warehouse_id_sku_id_avg_3_prior"),
        F.max("running_count").over(w_unit_price_rank_distinct_ope).alias("unit_price_rank_distinct_operator_in_warehouse_i_2"),
    )
)

summary = (
    windowed
    .groupBy("sku_is_active", "warehouse_id")
    .agg(
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(~F.col("sku_is_active"), F.col("unit_price_percent_rank_2_rank"))).alias("unit_price_percent_rank_2_rank_not_sku_is_active"),
        F.sum(F.when(F.col("sku_is_active"), F.col("running_count")).otherwise(0)).alias("running_count_sku_is_active_true"),
    )
)

result = (
    summary
    .filter(~F.col("sku_is_active"))
    .select(
        "sku_is_active",
        "operator_list",
        "n_rows",
        "unit_price_percent_rank_2_rank_not_sku_is_active",
        F.when(F.col("n_rows") >= 9, "large").when(F.col("n_rows") >= 6, "medium").otherwise("small").alias("n_rows_tier"),
    )
    .orderBy(
        F.col("unit_price_percent_rank_2_rank_not_sku_is_active").desc(),
        F.col("n_rows_tier").desc(),
    )
)
