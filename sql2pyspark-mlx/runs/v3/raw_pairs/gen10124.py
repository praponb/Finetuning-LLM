from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .unionByName(df_subscriptions.select(F.col("end_date").alias("opened_date")))
    .orderBy(F.col("opened_date").desc())
)
