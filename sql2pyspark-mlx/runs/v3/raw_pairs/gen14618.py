from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 80)
    .groupBy("customer_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.cnt") >= 7)
    .select(F.col("c.id"), F.col("c.name"), F.col("c.credit_limit"), F.col("cs.cnt"))
    .orderBy("id")
)
