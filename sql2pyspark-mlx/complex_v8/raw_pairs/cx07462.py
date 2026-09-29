from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("move_type") == "ADJUST")
    .select(
        F.col("id").alias("key_id"),
        "qty",
        F.col("moved_at").alias("happened_at"),
    )
)

move_rows = (
    df_stock_moves
    .filter((F.month(F.col("moved_at")).isin(1, 2)) & (F.col("qty") > 169))
    .select("warehouse_id", "sku_id", "moved_at")
)

combined = move_side.union(move_rows)

result = (
    combined
    .groupBy("key_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("qty").alias("total_qty"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("key_id")
)
