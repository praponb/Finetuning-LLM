from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .filter(F.hour(F.col("moved_at")).between(7, 11))
    .select(
        "warehouse_id",
        F.col("moved_at").alias("event_time"),
        F.col("id").alias("ref_id"),
    )
)

move_rows_2 = (
    df_stock_moves
    .filter((F.col("unit_price") >= 206.8) & (F.col("sku_id") <= 18))
    .select("warehouse_id", "moved_at", "qty")
)

result = move_rows.union(move_rows_2)
