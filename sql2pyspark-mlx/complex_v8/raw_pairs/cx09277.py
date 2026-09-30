from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-02-18 15:00:00").cast("timestamp"))
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

annotated = (
    cleaned
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "*",
        F.when(F.col("unit_price") >= 341.5, "A").when(F.col("unit_price") >= 160.8, "B").otherwise("C").alias("unit_price_tier"),
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

tagged = (
    annotated
    .filter(F.col("moved_at") < F.lit("2025-03-08 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.coalesce(F.col("unit_price_per_qty"), F.lit(0.0)).alias("unit_price_per_qty_filled"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.abs(F.col("unit_price") - 236.5).alias("unit_price_distance"),
    )
)

result = (
    tagged
    .groupBy("warehouse_id")
    .pivot("move_type", ["TRANSFER", "INBOUND", "ADJUST", "OUTBOUND"])
    .agg(F.sum("unit_price_per_qty"))
    .orderBy("warehouse_id")
)
