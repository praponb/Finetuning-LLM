from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .select(
        "sku_id",
        F.col("operator").alias("category"),
        F.col("moved_at").alias("happened_at"),
        F.lit("move").alias("record_type"),
    )
)

move_side_2 = (
    df_stock_moves
    .select(
        "warehouse_id",
        "move_type",
        "moved_at",
        F.lit("move_2").alias("record_type"),
    )
)

result = (
    move_side
    .union(move_side_2)
    .orderBy(F.col("record_type").desc())
)
