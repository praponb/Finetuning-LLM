from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["enterprise", "basic", "standard"])
    .agg(F.sum("monthly_fee").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
)
