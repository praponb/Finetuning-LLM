from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("unit_price") <= 329.7) & (F.col("move_type") != "OUTBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.month(F.col("moved_at")).alias("moved_at_month"),
        F.abs(F.col("unit_price") - 248.3).alias("unit_price_distance"),
    )
)

combined_details_sku = (
    cleaned.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.weight_kg").between(6.4, 21.0)), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.unit_price_distance",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        "sk.sku_code",
    )
)

result = (
    combined_details_sku
    .groupBy("move_type")
    .pivot("operator", ["op_ann", "op_eve"])
    .agg(F.min("sku_weight_kg"))
)
