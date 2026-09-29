from pyspark.sql import functions as F

move_rows = df_stock_moves.select(F.col("operator").alias("name"), "moved_at")

move_rows_2 = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("operator") == "op_ann"))
    .select(F.col("move_type").alias("name"), "moved_at")
)

result = (
    move_rows
    .unionByName(move_rows_2)
    .distinct()
)
