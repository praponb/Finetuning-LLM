from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter(F.col("unit_price") < 289.7)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
        F.round(F.col("unit_price") * 1.1, 2).alias("unit_price_adj"),
    )
)

totals = (
    filtered
    .filter(F.col("unit_price") >= 205.1)
    .groupBy(
        "move_type",
        F.date_trunc("month", "moved_at_date").alias("moved_at_date_month"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("qty_per_unit_price") / F.nullif(F.sum("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price_to_unit_price_ratio"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.max("unit_price_adj").alias("max_unit_price_adj_check"),
    )
    .filter(F.col("max_unit_price_adj_check") > 263.1)
    .drop("max_unit_price_adj_check")
)

enriched = (
    totals
    .select(
        "*",
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("distinct_ids").cast("string")).alias("move_type_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

screened = (
    enriched
    .crossJoin(enriched.agg(F.max("distinct_warehouse_ids").alias("overall_max_distinct_warehouse_ids")))
    .filter(F.col("distinct_warehouse_ids") < F.col("overall_max_distinct_warehouse_ids"))
    .drop("overall_max_distinct_warehouse_ids")
)

with_activity = (
    screened
    .crossJoin(screened.agg(F.avg("distinct_ids").alias("overall_avg_distinct_ids")))
    .filter(F.col("distinct_ids") > F.col("overall_avg_distinct_ids"))
    .drop("overall_avg_distinct_ids")
)

qualified = (
    with_activity
    .crossJoin(with_activity.agg(F.avg("qty_per_unit_price_to_unit_price_ratio").alias("overall_avg_qty_per_unit_price_to_unit_price_rat")))
    .filter(F.col("qty_per_unit_price_to_unit_price_ratio") > F.col("overall_avg_qty_per_unit_price_to_unit_price_rat"))
    .drop("overall_avg_qty_per_unit_price_to_unit_price_rat")
)

result = (
    qualified
    .groupBy("distinct_warehouse_ids")
    .pivot("move_type", ["TRANSFER", "OUTBOUND"])
    .agg(F.count("distinct_ids"))
    .orderBy("distinct_warehouse_ids")
)
