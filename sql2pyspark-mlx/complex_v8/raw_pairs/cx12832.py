from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("unit_price") >= 236.5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

move_rows = (
    cleaned_moves
    .filter((F.col("moved_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp")) & (F.col("warehouse_id") <= 4))
    .select(
        "operator_or_unknown",
        "id",
        F.col("move_type").alias("category"),
        F.lit("move").alias("origin"),
    )
)

sku_rows = (
    df_skus
    .select(
        F.col("sku_code").alias("operator_or_unknown"),
        "id",
        "category",
        F.lit("sku").alias("origin"),
    )
)

result = (
    move_rows
    .union(sku_rows)
    .distinct()
    .orderBy("operator_or_unknown")
)
