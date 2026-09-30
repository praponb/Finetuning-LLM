from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter((F.hour(F.col("moved_at")).between(8, 12)) & (F.col("warehouse_id").isNotNull()))
)

move_rows = (
    recent
    .select(
        "warehouse_id",
        F.col("unit_price").alias("metric"),
        "qty",
        F.lit("move").alias("origin"),
    )
)

move_side = (
    df_stock_moves
    .select(
        "id",
        "unit_price",
        "sku_id",
        F.lit("move_2").alias("origin"),
    )
)

result = move_rows.union(move_side)
