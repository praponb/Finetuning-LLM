from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp"))
    .select("move_type", F.col("sku_id").alias("key_id"))
)

move_rows = df_stock_moves.select(F.col("operator").alias("move_type"), F.col("id").alias("key_id"))

result = (
    move_side
    .unionByName(move_rows)
    .orderBy("move_type", "key_id")
)
