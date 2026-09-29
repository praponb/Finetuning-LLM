from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(80, 150))
    .withColumn("monthly_fee_filled", F.coalesce("monthly_fee", F.lit(0)))
    .withColumn("monthly_fee_filled_2", F.coalesce("monthly_fee", F.lit(0)))
    .orderBy("customer_id")
)
