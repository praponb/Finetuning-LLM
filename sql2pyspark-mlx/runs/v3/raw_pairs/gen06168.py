from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id").isNull() & F.col("i.product_id").isin([1, 4, 6, 10]))
    .select(F.col("i.id"), F.col("i.store_id"), F.col("p.launch_date"))
    .orderBy("launch_date")
)
