from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "EXPIRED")
    .withColumn("monthly_fee_whole", F.floor("monthly_fee"))
)
