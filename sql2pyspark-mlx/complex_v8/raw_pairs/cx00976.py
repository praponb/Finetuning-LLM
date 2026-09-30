from pyspark.sql import functions as F

cleaned = df_stock_moves

with_details = (
    cleaned.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select("s.*", F.col("w.wh_code").alias("warehouse_wh_code"))
)

result = (
    with_details
    .groupBy("moved_at", "sku_id")
    .pivot("move_type", ["INBOUND", "ADJUST", "TRANSFER", "OUTBOUND"])
    .agg(F.min("unit_price"))
    .orderBy("moved_at", "sku_id")
)
