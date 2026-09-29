from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") < 450)
    .select("stock_qty", F.explode("tags").alias("tag_name"))
    .groupBy("tag_name")
    .agg(F.sum("stock_qty").alias("total_stock"))
)
