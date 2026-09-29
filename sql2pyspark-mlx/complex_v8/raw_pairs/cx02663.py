from pyspark.sql import functions as F

move_rows = df_stock_moves.select("sku_id", "moved_at")

move_rows_2 = df_stock_moves.select("sku_id", "moved_at")

combined = (
    move_rows
    .union(move_rows_2)
    .distinct()
)

result = (
    combined
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("moved_at").alias("latest_moved_at"),
    )
    .orderBy("sku_id")
)
