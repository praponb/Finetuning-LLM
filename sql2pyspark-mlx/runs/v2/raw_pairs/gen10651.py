from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.max("end_date").alias("latest_end_date"),
        F.count("*").alias("row_count"),
    )
)
