from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 200)
    .withColumn("plan_prefix", F.substring("plan", 1, 3))
)

result = (
    cleaned
    .groupBy("customer_id")
    .pivot("plan", ["enterprise", "standard"])
    .agg(F.sum("monthly_fee").alias("total"))
)
