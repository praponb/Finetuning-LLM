from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .select("id", "status", "end_date", F.substring("plan", 1, 2).alias("plan_prefix"))
)
