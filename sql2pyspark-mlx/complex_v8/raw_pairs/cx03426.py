from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .filter((F.col("operator") == "op_dew") & (F.to_date(F.col("moved_at")) < F.lit("2025-03-08").cast("date")))
    .select("move_type", F.col("moved_at").alias("event_time"))
)

move_rows_2 = df_stock_moves.select("operator", "moved_at")

result = (
    move_rows
    .union(move_rows_2)
    .distinct()
    .orderBy(F.col("move_type").desc(), "event_time")
)
