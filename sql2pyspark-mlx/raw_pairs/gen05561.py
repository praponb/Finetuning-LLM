from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.col("plan") == "basic")
    .groupBy("customer_id")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.min_monthly_fee") > 110)
    .select(F.col("c.id"), F.col("c.credit_limit"), F.col("cs.min_monthly_fee"))
    .orderBy(F.col("credit_limit").desc())
)
