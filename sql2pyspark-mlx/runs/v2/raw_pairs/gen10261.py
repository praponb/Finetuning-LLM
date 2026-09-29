from pyspark.sql import functions as F

result = (
    df_subscriptions
    .withColumn("end_date_minus_57d", F.date_sub("end_date", 57))
    .orderBy(F.col("customer_id").desc())
)
