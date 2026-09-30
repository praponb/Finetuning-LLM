from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 340)
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(F.min("launch_date").alias("first_launch_date"))
    .orderBy("first_launch_date", "launch_month")
    .limit(3)
)
