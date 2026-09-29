from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(F.avg("price").alias("avg_price"))
    .orderBy("launch_month")
)
