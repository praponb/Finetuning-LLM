from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-03-02 10:00:00").cast("timestamp")) & (F.col("unit_price") < 363.6))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
    )
)

enriched = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("w.city").alias("warehouse_city"),
    )
)

without_matches = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
)

result = (
    without_matches
    .groupBy("sku_id", "operator")
    .pivot("warehouse_city", ["Bangkok", "Singapore"])
    .agg(F.min("sku_unit_cost"))
    .orderBy("sku_id", "operator")
)
