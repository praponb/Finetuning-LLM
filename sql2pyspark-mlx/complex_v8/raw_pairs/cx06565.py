from pyspark.sql import functions as F

move_side = df_stock_moves.select(F.col("qty").alias("ref_id"), F.col("moved_at").alias("event_time"))

move_rows = df_stock_moves.select(F.col("qty").alias("ref_id"), F.col("moved_at").alias("event_time"))

result = (
    move_side
    .unionByName(move_rows)
    .orderBy(F.col("ref_id").desc())
)
