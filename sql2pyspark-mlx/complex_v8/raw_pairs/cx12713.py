from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .select(
        F.col("id").alias("entity_id"),
        "unit_price",
        F.col("qty").alias("key_id"),
    )
)

move_side_2 = (
    df_stock_moves
    .filter((F.col("operator").isin("op_dew", "op_bee", "op_eve")) & (F.col("sku_id") >= 7))
    .select("qty", "unit_price", "sku_id")
)

result = (
    move_side
    .union(move_side_2)
    .distinct()
    .orderBy(F.col("key_id").desc())
)
