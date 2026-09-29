from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("move_type") != "TRANSFER"))
    .select(
        F.col("operator").alias("category"),
        F.col("move_type").alias("label"),
        "moved_at",
    )
)

move_side_2 = (
    df_stock_moves
    .filter(F.col("unit_price") > 164.1)
    .select("move_type", "operator", "moved_at")
)

result = (
    move_side
    .union(move_side_2)
    .orderBy(F.col("label").desc(), "moved_at")
)
