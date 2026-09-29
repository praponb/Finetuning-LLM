from pyspark.sql import functions as F

prepared = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("unit_price") >= 328.3, "HIGH").when(F.col("unit_price") >= 238.5, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
    )
)

qualified = (
    prepared.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.moved_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp")), "left_semi")
)

result = (
    qualified
    .groupBy("operator")
    .pivot("move_type", ["TRANSFER", "OUTBOUND", "ADJUST", "INBOUND"])
    .agg(F.count("qty"))
    .orderBy("operator")
)
