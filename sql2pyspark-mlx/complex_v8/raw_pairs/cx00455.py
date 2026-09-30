from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .filter(F.col("qty") >= 78)
    .select(
        F.lit("move").alias("source"),
        F.col("sku_id").alias("key_id"),
        "moved_at",
        "unit_price",
    )
)

move_rows_2 = (
    df_stock_moves
    .select(
        F.lit("move_2").alias("source"),
        "sku_id",
        "moved_at",
        "unit_price",
    )
)

all_events = (
    move_rows
    .union(move_rows_2)
    .distinct()
)

result = (
    all_events
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("key_id").alias("total_key_id"),
        F.max("moved_at").alias("latest_moved_at"),
    )
    .orderBy("source")
)
