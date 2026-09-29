from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter(F.hour(F.col("moved_at")).between(7, 10))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 0.9, 2).alias("unit_price_adj"),
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
        F.when(F.col("unit_price") >= 292.3, "HIGH").when(F.col("unit_price") >= 180.0, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
    )
)

rollup_base = (
    valid_moves
    .groupBy("sku_id", F.to_date("moved_at").alias("moved_at_day"))
    .agg(
        F.countDistinct("operator").alias("distinct_operators"),
        F.round(F.sum("unit_price_adj"), 2).alias("total_unit_price_adj"),
        F.round(100 * F.count(F.when(F.col("operator").isNotNull(), 1)) / F.count("*"), 1).alias("pct_operator_is_not_null"),
    )
)

with_details = (
    rollup_base.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select("s.*", F.col("sk.labels").alias("sku_labels"))
)

result = (
    with_details
    .filter(F.col("distinct_operators") < 8)
    .select(
        "sku_id",
        "moved_at_day",
        "distinct_operators",
        "sku_labels",
        F.round(F.col("total_unit_price_adj") / F.nullif(F.col("distinct_operators"), F.lit(0)), 3).alias("total_unit_price_adj_per_distinct_operators"),
        F.add_months(F.col("moved_at_day"), 6).alias("moved_at_day_plus_6m"),
    )
    .orderBy(F.col("distinct_operators").desc())
)
