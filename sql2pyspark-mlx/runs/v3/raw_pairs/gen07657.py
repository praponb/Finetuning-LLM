from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "customer_id", F.upper("plan").alias("plan_upper"))
)
