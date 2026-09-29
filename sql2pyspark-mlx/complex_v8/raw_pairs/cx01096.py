from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
    )
)

move_side = (
    df_stock_moves
    .filter((F.col("warehouse_id") <= 5) & (F.col("unit_price") >= 80.5))
    .select(
        "sku_id",
        F.col("moved_at").alias("event_time"),
        F.col("unit_price").alias("value"),
        F.lit("move").alias("record_type"),
    )
)

move_side_2 = (
    df_stock_moves
    .select(
        "id",
        "moved_at",
        "unit_price",
        F.lit("move_2").alias("record_type"),
    )
)

result = move_side.union(move_side_2)
