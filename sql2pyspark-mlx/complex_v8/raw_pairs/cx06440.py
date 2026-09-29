from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter((F.col("operator") != "op_dew") & (F.col("unit_price") <= 372.6))
)

move_side = (
    cleaned_moves
    .filter(F.col("moved_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp"))
    .select(
        "sku_id",
        "qty",
        "moved_at",
        F.lit("move").alias("origin"),
    )
)

move_rows = (
    df_stock_moves
    .select(
        "qty",
        "sku_id",
        "moved_at",
        F.lit("move_2").alias("origin"),
    )
)

result = (
    move_side
    .union(move_rows)
    .distinct()
    .orderBy(F.col("qty").desc(), "origin")
)
