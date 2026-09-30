from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([4, 5, 10, 14]))
    .withColumn("monthly_fee_whole", F.floor("monthly_fee"))
    .withColumn("end_date_period", F.date_trunc("YEAR", "end_date"))
)
