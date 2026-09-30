from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["standard", "enterprise"])
    .agg(F.sum("monthly_fee").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
