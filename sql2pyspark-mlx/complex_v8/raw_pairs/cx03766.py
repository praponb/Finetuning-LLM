from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        F.lit("move").alias("origin"),
        F.col("move_type").alias("name"),
        "moved_at",
        F.col("operator").alias("category"),
    )
)

move_side = (
    df_stock_moves
    .filter(F.col("unit_price") > 189.4)
    .select(
        F.lit("move_2").alias("origin"),
        F.col("move_type").alias("name"),
        "moved_at",
        F.col("operator").alias("category"),
    )
)

result = (
    move_rows
    .union(move_side)
    .orderBy(F.col("category").desc())
)
