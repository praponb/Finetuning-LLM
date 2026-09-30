from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") < "2024-10-01")
    .select(F.col("launch_date").alias("event_date"))
    .unionByName(df_subscriptions.select(F.col("start_date").alias("event_date")))
    .orderBy(F.col("event_date").desc())
)
