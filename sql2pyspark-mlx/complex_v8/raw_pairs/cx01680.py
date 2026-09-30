from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) | (F.col("warehouse_id") >= 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
    )
)

move_side = (
    cleaned
    .select(
        F.lit("move").alias("source"),
        "sku_id",
        F.col("move_type").alias("label"),
        F.col("moved_at").alias("event_time"),
    )
)

move_rows = (
    df_stock_moves
    .select(
        F.lit("move_2").alias("source"),
        "qty",
        "move_type",
        "moved_at",
    )
)

move_rows_2 = (
    df_stock_moves
    .select(
        F.lit("move_3").alias("source"),
        "warehouse_id",
        "move_type",
        "moved_at",
    )
)

result = (
    move_side
    .union(move_rows)
    .union(move_rows_2)
    .orderBy(F.col("sku_id").desc(), "label")
)
