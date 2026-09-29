from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") >= "2023-06-08")
    .withColumn("launch_date_month_end", F.last_day("launch_date"))
    .orderBy("price", "id")
    .limit(1)
)
