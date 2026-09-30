from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .filter(F.col("plan").isin(["standard", "basic", "enterprise"]))
    .groupBy("customer_id")
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("C"), F.col("c.id") == F.col("C.customer_id"), "left")
    .select(F.col("c.name"), F.col("c.credit_limit"), F.col("C.stddev_monthly_fee"))
    .orderBy("credit_limit")
)
