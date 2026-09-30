from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .select(
        F.lit("move").alias("src"),
        "warehouse_id",
        F.col("moved_at").alias("happened_at"),
    )
)

move_rows = (
    df_stock_moves
    .filter(~F.col("move_type").isin("INBOUND", "TRANSFER"))
    .select(
        F.lit("move_2").alias("src"),
        F.col("id").alias("warehouse_id"),
        F.col("moved_at").alias("happened_at"),
    )
)

result = (
    move_side
    .unionByName(move_rows)
    .distinct()
)
