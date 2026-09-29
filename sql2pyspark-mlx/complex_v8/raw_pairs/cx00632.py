from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.month(F.col("moved_at")).isin(1, 3))
    .select("operator", F.col("qty").alias("key_id"), "moved_at")
)

move_rows = (
    df_stock_moves
    .filter(F.col("operator").isNotNull())
    .select(
        F.col("move_type").alias("operator"),
        F.col("sku_id").alias("key_id"),
        "moved_at",
    )
)

stacked = move_side.unionByName(move_rows)

result = (
    stacked
    .groupBy("operator")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("key_id").alias("total_key_id"),
        F.max("moved_at").alias("latest_moved_at"),
    )
    .orderBy("operator")
)
