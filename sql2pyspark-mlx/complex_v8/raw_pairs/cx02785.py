from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter(F.col("move_type") != "OUTBOUND")
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

move_side = (
    prepared_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("move_type") == "TRANSFER"))
    .select(F.col("id").alias("ref_id"), "moved_at")
)

move_side_2 = df_stock_moves.select(F.col("id").alias("ref_id"), "moved_at")

result = (
    move_side
    .union(move_side_2)
    .distinct()
    .orderBy(F.col("ref_id").desc())
)
