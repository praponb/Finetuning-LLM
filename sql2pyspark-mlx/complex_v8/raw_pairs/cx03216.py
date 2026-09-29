from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter(F.col("unit_price") <= 396.4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

move_side = (
    valid_moves
    .filter(F.col("warehouse_id") <= 5)
    .select("sku_id", F.col("moved_at").alias("event_time"))
)

move_rows = (
    df_stock_moves
    .filter((F.col("move_type") != "ADJUST") & (F.col("moved_at") >= F.lit("2025-02-12 11:00:00").cast("timestamp")))
    .select("sku_id", F.col("moved_at").alias("event_time"))
)

all_events = move_side.unionByName(move_rows)

result = (
    all_events
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("sku_id")
)
