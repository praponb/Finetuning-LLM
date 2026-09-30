from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
    .select("operator", F.col("warehouse_id").alias("entity_id"))
)

sku_rows = (
    df_skus
    .filter((F.col("weight_kg") > 6.4) & (~F.col("is_active")))
    .select("sku_code", "id")
)

result = (
    move_side
    .union(sku_rows)
    .orderBy("operator", "entity_id")
)
