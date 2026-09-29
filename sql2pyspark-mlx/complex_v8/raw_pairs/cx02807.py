from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-03-02 10:00:00").cast("timestamp"))
    .select("sku_id", F.col("operator").alias("label"), "unit_price")
)

sku_side = (
    df_skus
    .filter((F.col("unit_cost") < 251.2) & (F.upper(F.trim(F.col("sku_code"))).like("E%")))
    .select("id", "sku_code", "weight_kg")
)

result = (
    move_side
    .union(sku_side)
    .distinct()
    .orderBy("label", "unit_price")
)
