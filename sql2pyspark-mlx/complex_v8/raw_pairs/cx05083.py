from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_warehouse_ids = Window.partitionBy("distinct_warehouse_ids")
w_all_by_distinct_sku_ids_prio = Window.orderBy(F.col("distinct_sku_ids").desc(), "moved_at_ym").rowsBetween(-6, -1)
w_all_by_distinct_sku_ids_runn = Window.orderBy(F.col("distinct_sku_ids").desc(), "moved_at_ym").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_warehouse_ids_by_n = Window.partitionBy("distinct_warehouse_ids").orderBy(F.col("n_rows_plus_distinct_sku_ids").desc())
w_first_known_distinct_warehou = Window.partitionBy("first_known_distinct_warehouse_ids", "distinct_warehouse_ids").orderBy(F.col("n_rows").asc_nulls_last())
w_all_by_move_type_list_count = Window.orderBy(F.col("move_type_list_count_avg_6_prior").desc())
w_first_known_distinct_warehou_2 = Window.partitionBy("first_known_distinct_warehouse_ids", "distinct_warehouse_ids").orderBy(F.col("n_rows").asc_nulls_last(), "moved_at_ym")

scoped_moves = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.when(F.col("unit_price") >= 317.0, "large").when(F.col("unit_price") >= 150.7, "medium").otherwise("small").alias("unit_price_tier"),
    )
)

scored = (
    scoped_moves
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "*",
        F.substring(F.trim(F.col("operator")), 1, 4).alias("operator_prefix"),
        F.hour(F.col("moved_at")).alias("moved_at_hour"),
    )
)

rollup_base = (
    scored
    .groupBy(F.date_format("moved_at", "yyyy-MM").alias("moved_at_ym"))
    .agg(
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.round(F.sum("unit_price") / F.nullif(F.sum("qty_minus_unit_price"), F.lit(0)), 3).alias("unit_price_to_qty_minus_unit_price_ratio"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
    )
)

annotated = (
    rollup_base
    .filter(F.col("distinct_sku_ids") < 8)
    .select(
        "*",
        F.size(F.col("move_type_list")).alias("move_type_list_count"),
        (F.col("n_rows") + F.col("distinct_sku_ids")).alias("n_rows_plus_distinct_sku_ids"),
    )
)

metrics = (
    annotated
    .select(
        "*",
        F.round(F.col("n_rows") / F.nullif(F.sum("n_rows").over(w_distinct_warehouse_ids), F.lit(0)), 4).alias("n_rows_share_of_distinct_warehouse_ids"),
        F.round(F.avg("move_type_list_count").over(w_all_by_distinct_sku_ids_prio), 2).alias("move_type_list_count_avg_6_prior"),
        F.first("distinct_warehouse_ids", ignorenulls=True).over(w_all_by_distinct_sku_ids_runn).alias("first_known_distinct_warehouse_ids"),
        F.round(F.cume_dist().over(w_distinct_warehouse_ids_by_n), 3).alias("n_rows_plus_distinct_sku_ids_cume_dist"),
        F.first("distinct_sku_ids", ignorenulls=True).over(w_all_by_distinct_sku_ids_runn).alias("first_known_distinct_sku_ids"),
    )
)

without_matches = (
    metrics
    .crossJoin(metrics.agg(F.avg("first_known_distinct_warehouse_ids").alias("overall_avg_first_known_distinct_warehouse_ids")))
    .filter(F.col("first_known_distinct_warehouse_ids") > F.col("overall_avg_first_known_distinct_warehouse_ids"))
    .drop("overall_avg_first_known_distinct_warehouse_ids")
)

ranked = (
    without_matches
    .select(
        "moved_at_ym",
        "n_rows",
        "move_type_list",
        "distinct_warehouse_ids",
        "n_rows_plus_distinct_sku_ids",
        "n_rows_share_of_distinct_warehouse_ids",
        "move_type_list_count_avg_6_prior",
        "first_known_distinct_warehouse_ids",
        "first_known_distinct_sku_ids",
        F.dense_rank().over(w_first_known_distinct_warehou).alias("n_rows_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_move_type_list_count), 3).alias("move_type_list_count_avg_6_prior_percent_rank"),
        F.lead("distinct_sku_ids", 1, 0).over(w_first_known_distinct_warehou_2).alias("next_distinct_sku_ids"),
    )
)

result = (
    ranked
    .select(
        "moved_at_ym",
        "n_rows",
        "move_type_list",
        "distinct_warehouse_ids",
        "n_rows_share_of_distinct_warehouse_ids",
        "move_type_list_count_avg_6_prior",
        "first_known_distinct_warehouse_ids",
        "n_rows_dense_rank",
        "move_type_list_count_avg_6_prior_percent_rank",
        "next_distinct_sku_ids",
        F.round(F.col("next_distinct_sku_ids") / F.nullif(F.col("n_rows"), F.lit(0)), 3).alias("next_distinct_sku_ids_per_n_rows"),
        F.array_join(F.array_sort(F.col("move_type_list")), ",").alias("move_type_list_text"),
    )
    .orderBy(
        F.col("next_distinct_sku_ids_per_n_rows").desc(),
        F.col("move_type_list_count_avg_6_prior_percent_rank").desc_nulls_first(),
    )
)
