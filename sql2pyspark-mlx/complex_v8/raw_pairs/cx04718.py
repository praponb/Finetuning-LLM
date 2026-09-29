from pyspark.sql import functions as F

sku_rows = (
    df_skus
    .select(
        "category",
        "sku_code",
        F.col("unit_cost").alias("amount"),
        F.lit("sku").alias("source"),
    )
)

move_side = (
    df_stock_moves
    .select(
        "operator",
        "move_type",
        "unit_price",
        F.lit("move").alias("source"),
    )
)

result = (
    sku_rows
    .union(move_side)
    .orderBy("source", "sku_code")
)
