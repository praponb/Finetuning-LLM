from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .unionByName(df_subscriptions.select(F.col("end_date").alias("launch_date")))
)
