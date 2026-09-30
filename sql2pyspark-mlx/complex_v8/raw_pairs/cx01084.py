from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter((F.col("qty").between(145, 393)) & (F.col("moved_at") < F.lit("2025-02-18 15:00:00").cast("timestamp")))
    .select(
        F.col("warehouse_id").alias("key_id"),
        "unit_price",
        F.lit("move").alias("origin"),
    )
)

sku_side = (
    df_skus
    .filter((~F.col("is_active")) & (F.col("unit_cost") > 108.5))
    .select("id", "unit_cost", F.lit("sku").alias("origin"))
)

result = move_side.union(sku_side)
