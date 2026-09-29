from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id") == 23)
    .groupBy(F.col("p.brand"))
    .agg(
        F.count("*").alias("inventory_count"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .filter(F.col("inventory_count") >= 1)
    .orderBy("brand")
    .limit(50)
)
