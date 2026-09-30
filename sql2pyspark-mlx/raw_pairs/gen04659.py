from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.sum("units").alias("total_units"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(F.col("p.stock_qty"), F.col("p.price"), F.col("ps.total_units"))
    .orderBy(F.col("price").desc())
)
