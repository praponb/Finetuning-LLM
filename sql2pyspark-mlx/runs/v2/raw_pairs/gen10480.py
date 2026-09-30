from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 140)
    .withColumn("monthly_fee_adjusted", F.round((F.col("monthly_fee") * 100), 2))
    .orderBy(F.col("customer_id").desc())
)
