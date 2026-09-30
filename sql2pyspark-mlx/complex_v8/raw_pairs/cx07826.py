from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("qty") >= 116) & (F.col("unit_price") <= 298.9))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

per_group = (
    cleaned
    .groupBy("move_type")
    .agg(
        F.sum(F.when(F.col("operator").isNotNull(), 1).otherwise(0)).alias("n_operator_is_not_null"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.round(100 * F.count(F.when(F.col("move_type").isNotNull(), 1)) / F.count("*"), 1).alias("pct_move_type_is_not_null"),
        F.count("*").alias("cnt"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
    )
    .filter(F.col("cnt") >= 2)
)

labeled = (
    per_group
    .filter(F.col("cnt") < 11)
    .select(
        "*",
        F.concat(F.col("move_type"), F.lit("#"), F.col("distinct_warehouse_ids").cast("string")).alias("move_type_ref"),
        (F.col("cnt") + F.col("n_operator_is_not_null")).alias("cnt_plus_n_operator_is_not_null"),
    )
)

exploded = (
    labeled
    .select(
        "move_type",
        "n_operator_is_not_null",
        "pct_move_type_is_not_null",
        "cnt",
        "distinct_warehouse_ids",
        "move_type_ref",
        "cnt_plus_n_operator_is_not_null",
        F.explode_outer("move_type_list").alias("move_type_list_item"),
    )
)

without_matches = (
    exploded
    .crossJoin(exploded.agg(F.max("n_operator_is_not_null").alias("overall_max_n_operator_is_not_null")))
    .filter(F.col("n_operator_is_not_null") < F.col("overall_max_n_operator_is_not_null"))
    .drop("overall_max_n_operator_is_not_null")
)

result = (
    without_matches
    .groupBy("cnt")
    .pivot("move_type", ["OUTBOUND", "INBOUND", "TRANSFER"])
    .agg(F.avg("n_operator_is_not_null").alias("avg_v"), F.max("n_operator_is_not_null").alias("max_v"))
    .orderBy("cnt")
)
