from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([13, 21, 31, 32]))
    .select("id", "end_date", F.length("plan").alias("plan_length"))
)
