from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([28, 29, 32, 35]))
    .select("stock_qty", F.explode("tags").alias("tag_name"))
    .groupBy("tag_name")
    .agg(F.sum("stock_qty").alias("total_stock"))
    .orderBy(F.col("total_stock").desc(), "tag_name")
)
