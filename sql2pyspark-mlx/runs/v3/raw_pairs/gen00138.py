from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([5, 9, 12]))
    .select("stock_qty", F.explode("tags").alias("tag_name"))
    .groupBy("tag_name")
    .agg(F.sum("stock_qty").alias("total_stock"))
)
