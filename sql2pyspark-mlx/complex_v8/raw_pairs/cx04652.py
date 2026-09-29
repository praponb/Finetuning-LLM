from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .select(
        "city",
        F.col("capacity").alias("entity_id"),
        F.lit("warehouse").alias("source"),
    )
)

sku_rows = (
    df_skus
    .filter(~F.col("is_active"))
    .select(
        F.col("category").alias("city"),
        F.col("id").alias("entity_id"),
        F.lit("sku").alias("source"),
    )
)

move_rows = (
    df_stock_moves
    .select(
        F.col("operator").alias("city"),
        F.col("qty").alias("entity_id"),
        F.lit("move").alias("source"),
    )
)

result = (
    warehouse_rows
    .unionByName(sku_rows)
    .unionByName(move_rows)
    .orderBy("source")
)
