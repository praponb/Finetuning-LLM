from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Pro%"))
    .select("stock_qty", F.explode("tags").alias("tag_name"))
    .groupBy("tag_name")
    .agg(F.sum("stock_qty").alias("total_stock"))
)
