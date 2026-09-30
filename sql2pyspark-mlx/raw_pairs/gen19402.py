from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 1) & (F.col("monthly_fee") > 110))
    .groupBy(F.year("end_date").alias("end_year"))
    .agg(
        F.max("start_date").alias("last_start_date"),
        F.count("*").alias("subscription_count"),
    )
    .orderBy("last_start_date", "end_year")
    .limit(10)
)
