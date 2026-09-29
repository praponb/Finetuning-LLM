from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull() & (F.col("monthly_fee") < 130))
    .select("id", "customer_id", F.upper("plan").alias("plan_upper"))
    .orderBy(F.col("id").desc())
)
