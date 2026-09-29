from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
    )
)

result = (
    scoped_moves
    .filter(F.col("unit_price") <= 393.3)
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "unit_price",
    )
)
