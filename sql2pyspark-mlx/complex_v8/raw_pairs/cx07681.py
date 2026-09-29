from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

scored = (
    recent
    .select(
        "id",
        "sku_id",
        "unit_price",
        "moved_at_date",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

enriched_sku = (
    scored.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.unit_price",
        "s.moved_at_date",
        "s.unit_price_filled",
        F.col("sk.category").alias("sku_category"),
    )
)

per_group = (
    enriched_sku
    .groupBy(
        "sku_id",
        "sku_category",
        F.date_trunc("month", "moved_at_date").alias("moved_at_date_month"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.max("moved_at_date").alias("last_moved_at_date"),
    )
)

scored_2 = (
    per_group
    .filter(F.col("last_moved_at_date").between(F.lit("2024-08-01").cast("date"), F.lit("2025-08-01").cast("date")))
    .select(
        "*",
        F.upper(F.trim(F.col("sku_category"))).alias("sku_category_clean"),
        F.substring(F.trim(F.col("sku_category")), 1, 2).alias("sku_category_prefix"),
    )
)

derived = (
    scored_2
    .filter(F.col("last_moved_at_date").between(F.lit("2024-02-01").cast("date"), F.lit("2025-05-01").cast("date")))
    .withColumn("cnt_tier", F.when(F.col("cnt") >= 10, "HIGH").when(F.col("cnt") >= 5, "MEDIUM").otherwise("LOW"))
)

result = (
    derived
    .select(
        "sku_id",
        "sku_category",
        "distinct_ids",
        "cnt",
        "sku_category_clean",
        "sku_category_prefix",
        F.concat_ws(" / ", F.col("cnt_tier"), F.col("sku_category_prefix")).alias("cnt_tier_sku_category_prefix_label"),
    )
)
