from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter((F.to_date(F.col("moved_at")) < F.lit("2025-02-18").cast("date")) | (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
    )
)

move_side = (
    valid
    .filter((F.col("warehouse_id") <= 4) & (F.hour(F.col("moved_at")).between(9, 14)))
    .select("operator", "qty_minus_unit_price", F.lit("move").alias("record_type"))
)

move_side_2 = df_stock_moves.select("move_type", "unit_price", F.lit("move_2").alias("record_type"))

result = move_side.union(move_side_2)
