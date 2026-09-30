from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((F.col("opened_on") >= F.lit("2024-02-19").cast("date")) & (F.lower(F.col("wh_code")).like("%l1%")))
    .select(
        F.col("id").alias("entity_id"),
        F.col("city").alias("name"),
        F.lit("warehouse").alias("record_type"),
    )
)

move_side = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-02-12 10:00:00").cast("timestamp")) & (F.col("warehouse_id") >= 4))
    .select("warehouse_id", "move_type", F.lit("move").alias("record_type"))
)

sku_rows = df_skus.select("id", "sku_code", F.lit("sku").alias("record_type"))

result = (
    warehouse_side
    .union(move_side)
    .union(sku_rows)
    .orderBy("record_type")
)
