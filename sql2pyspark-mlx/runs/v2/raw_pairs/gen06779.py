from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg(F.count("*").alias("num_inventory"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(F.col("p.product_name"), F.col("p.price"), F.col("t.num_inventory"))
)
