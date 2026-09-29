from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter(F.col("city").isin("Kuala Lumpur", "Bangkok"))
    .select(F.col("capacity").alias("qty"), F.col("city").alias("label"))
)

move_side = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-02-24 12:00:00").cast("timestamp"))
    .select(F.col("sku_id").alias("qty"), F.col("operator").alias("label"))
)

result = (
    warehouse_side
    .union(move_side)
    .orderBy(F.col("qty").desc())
)
