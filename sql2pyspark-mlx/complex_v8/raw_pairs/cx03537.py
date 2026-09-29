from pyspark.sql import functions as F

move_rows = df_stock_moves.select(F.col("id").alias("entity_id"), "sku_id")

move_rows_2 = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")) & (F.col("unit_price") < 337.6))
    .select(F.col("qty").alias("entity_id"), F.col("warehouse_id").alias("sku_id"))
)

combined = move_rows.unionByName(move_rows_2)

result = (
    combined
    .groupBy("entity_id")
    .agg(F.count("*").alias("n_rows"), F.sum("sku_id").alias("total_sku_id"))
    .orderBy("entity_id")
)
