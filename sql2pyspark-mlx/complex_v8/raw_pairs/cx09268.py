from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter(F.month(F.col("moved_at")).isin(1, 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

move_side = (
    filtered
    .filter((F.col("unit_price").between(136.0, 361.9)) & (F.col("sku_id").isNotNull()))
    .select(F.col("warehouse_id").alias("qty"))
)

move_side_2 = df_stock_moves.select("qty")

result = (
    move_side
    .subtract(move_side_2)
    .orderBy("qty")
)
