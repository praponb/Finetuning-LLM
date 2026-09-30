from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter(F.col("move_type") == "TRANSFER")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.when(F.col("qty") >= 372, "HIGH").when(F.col("qty") >= 169, "MEDIUM").otherwise("LOW").alias("qty_tier"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
    )
)

labeled = (
    recent
    .select(
        "*",
        F.greatest(F.col("qty"), F.col("qty_bucket")).alias("greatest_qty_qty_bucket"),
        (F.col("moved_at") + F.expr("INTERVAL 30 MINUTES")).alias("moved_at_plus_30m"),
    )
)

annotated = (
    labeled
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("moved_at")) - F.unix_timestamp(F.col("moved_at_plus_30m"))) / 60, 1).alias("minutes_moved_at_plus_30m_to_moved_at"),
        F.coalesce(F.col("qty_tier"), F.lit("UNKNOWN")).alias("qty_tier_or_unknown"),
    )
)

qty_tier_stats = (
    annotated
    .groupBy("qty_tier")
    .agg(F.max("greatest_qty_qty_bucket").alias("max_greatest_qty_qty_bucket"))
)

compared = (
    annotated.alias("s")
    .join(qty_tier_stats.alias("g"), F.col("g.qty_tier") == F.col("s.qty_tier"), "inner")
    .filter(F.col("s.greatest_qty_qty_bucket") == F.col("g.max_greatest_qty_qty_bucket"))
    .select(
        "s.*",
        "g.max_greatest_qty_qty_bucket",
        F.round(F.col("s.greatest_qty_qty_bucket") - F.col("g.max_greatest_qty_qty_bucket"), 2).alias("greatest_qty_qty_bucket_minus_max"),
    )
)

result = (
    compared
    .select(
        "id",
        "moved_at",
        "greatest_qty_qty_bucket",
        "moved_at_plus_30m",
        "minutes_moved_at_plus_30m_to_moved_at",
        "max_greatest_qty_qty_bucket",
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
    )
    .orderBy("moved_at")
)
