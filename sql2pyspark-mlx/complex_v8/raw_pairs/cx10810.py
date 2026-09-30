from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("unit_price") < 396.4))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

move_side = cleaned.select(F.col("warehouse_id").alias("qty"), F.col("unit_price").alias("metric"))

move_side_2 = (
    df_stock_moves
    .filter(F.col("unit_price").between(160.8, 434.1))
    .select("sku_id", "unit_price")
)

result = (
    move_side
    .union(move_side_2)
    .distinct()
)
