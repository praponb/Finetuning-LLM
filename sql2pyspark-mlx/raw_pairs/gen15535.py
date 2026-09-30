from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .subtract(df_subscriptions.select(F.col("end_date").alias("opened_date")))
)
