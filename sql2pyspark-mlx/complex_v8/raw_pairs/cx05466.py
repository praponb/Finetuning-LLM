from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-03-02 10:00:00").cast("timestamp"))
    .select(F.col("id").alias("qty"), F.col("unit_price").alias("amount"))
)

move_side_2 = (
    df_stock_moves
    .filter((F.col("sku_id") >= 2) & (F.col("operator") == "op_dew"))
    .select("sku_id", "unit_price")
)

result = (
    move_side
    .union(move_side_2)
    .distinct()
    .orderBy("qty")
)
