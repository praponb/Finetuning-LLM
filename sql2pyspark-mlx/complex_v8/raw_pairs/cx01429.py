from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter((F.col("sku_id") <= 13) & (F.col("moved_at") >= F.lit("2025-01-13 09:00:00").cast("timestamp")))
    .select(
        F.col("id").alias("ref_id"),
        F.col("moved_at").alias("happened_at"),
        F.col("move_type").alias("label"),
    )
)

move_rows = (
    df_stock_moves
    .filter((F.col("operator") == "op_ann") & (F.col("sku_id") <= 14))
    .select("warehouse_id", "moved_at", "move_type")
)

move_side_2 = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .select("id", "moved_at", "operator")
)

unioned = (
    move_side
    .union(move_rows)
    .union(move_side_2)
)

result = (
    unioned
    .groupBy("label")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ref_id").alias("total_ref_id"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("label")
)
