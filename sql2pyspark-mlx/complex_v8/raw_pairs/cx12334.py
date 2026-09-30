from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .filter(F.col("operator").isNotNull())
    .select(
        F.col("warehouse_id").alias("entity_id"),
        "moved_at",
        F.col("move_type").alias("category"),
    )
)

move_side = (
    df_stock_moves
    .filter((F.col("unit_price").between(111.7, 367.7)) & (F.col("qty") <= 393))
    .select(
        F.col("qty").alias("entity_id"),
        "moved_at",
        F.col("move_type").alias("category"),
    )
)

move_side_2 = (
    df_stock_moves
    .select(
        F.col("id").alias("entity_id"),
        "moved_at",
        F.col("move_type").alias("category"),
    )
)

result = (
    move_rows
    .unionByName(move_side)
    .unionByName(move_side_2)
)
