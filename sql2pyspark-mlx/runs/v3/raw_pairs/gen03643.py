from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date")
    .unionByName(df_stores.select(F.col("opened_date").alias("end_date")))
    .orderBy("end_date")
)
