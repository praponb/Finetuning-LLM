from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("num_subscriptions"),
        F.count("*").alias("cnt"),
        F.max("end_date").alias("latest_end_date"),
    )
)
