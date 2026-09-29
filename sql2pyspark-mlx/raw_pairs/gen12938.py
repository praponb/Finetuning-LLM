from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", F.month("launch_date").alias("launch_month"))
    .agg(F.sum("price").alias("total_price"))
    .filter(F.col("total_price") < 5000)
    .select("category", "launch_month", "total_price")
)
