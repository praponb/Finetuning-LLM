from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter((F.col("qty") <= 357) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("move_type")), 1, 4).alias("move_type_prefix"),
    )
)

enriched = (
    filtered
    .filter(F.col("move_type") != "ADJUST")
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "move_type_prefix",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
        F.when(F.col("unit_price") >= 298.9, "HIGH").when(F.col("unit_price") >= 152.7, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
        F.concat(F.col("move_type_prefix"), F.lit("#"), F.col("id").cast("string")).alias("move_type_prefix_ref"),
    )
)

stats = (
    enriched
    .groupBy("sku_id", "unit_price_tier")
    .agg(
        F.round(F.avg("unit_price_bucket"), 2).alias("avg_unit_price_bucket"),
        F.countDistinct("unit_price_tier").alias("distinct_unit_price_tiers"),
        F.avg("unit_price_bucket").alias("avg_unit_price_bucket_check"),
    )
    .filter(F.col("avg_unit_price_bucket_check") > 126)
    .drop("avg_unit_price_bucket_check")
)

result = (
    stats
    .select("sku_id", "unit_price_tier", "distinct_unit_price_tiers")
    .orderBy(F.col("unit_price_tier").desc(), "sku_id")
    .limit(5)
)
