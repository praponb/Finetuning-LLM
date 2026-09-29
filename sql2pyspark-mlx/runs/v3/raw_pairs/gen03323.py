from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-08-01", "2024-01-16"))
    .select(F.col("monthly_fee").alias("value"))
    .unionByName(df_payments.select(F.col("amount").alias("value")))
    .distinct()
    .orderBy("value")
)
