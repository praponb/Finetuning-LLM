from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.lit("subscription").alias("kind"), "monthly_fee")
    .union(df_payments.select(F.lit("payment").alias("kind"), "amount"))
    .orderBy("monthly_fee")
)
