from pyspark.sql import functions as F

result = (
    df_stores
    .select(F.col("opened_date").alias("event_date"))
    .unionByName(df_subscriptions.select(F.col("end_date").alias("event_date")))
    .orderBy("event_date")
)
