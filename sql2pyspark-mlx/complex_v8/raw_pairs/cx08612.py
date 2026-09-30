from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .select(
        "id",
        "sku_id",
        "unit_price",
        F.lit("move").alias("source"),
    )
)

move_side_2 = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("move_type") == "OUTBOUND"))
    .select(
        "sku_id",
        "qty",
        "unit_price",
        F.lit("move_2").alias("source"),
    )
)

result = (
    move_side
    .union(move_side_2)
    .orderBy(F.col("unit_price").desc())
)
