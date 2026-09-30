from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter((F.col("unit_price") >= 86.3) & (F.col("move_type").isin("OUTBOUND", "INBOUND", "ADJUST")))
    .select(
        F.col("operator").alias("label"),
        "moved_at",
        F.col("move_type").alias("name"),
        F.lit("move").alias("source"),
    )
)

move_rows = (
    df_stock_moves
    .select(
        F.col("move_type").alias("label"),
        "moved_at",
        F.col("operator").alias("name"),
        F.lit("move_2").alias("source"),
    )
)

result = (
    move_side
    .unionByName(move_rows)
    .orderBy(F.col("moved_at").desc())
)
