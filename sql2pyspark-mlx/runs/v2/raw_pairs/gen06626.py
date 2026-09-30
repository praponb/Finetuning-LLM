from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter(F.col("price").between(360, 1080) & (F.col("supplier_id") == 33))
    .withColumn("category_product_name", F.concat("category", F.lit(" - "), "product_name"))
)

agg = (
    filtered_products
    .groupBy("category")
    .agg(F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"))
)

result = (
    agg
    .filter(F.col("avg_stock_qty") < 67)
    .orderBy(F.col("category").desc())
)
