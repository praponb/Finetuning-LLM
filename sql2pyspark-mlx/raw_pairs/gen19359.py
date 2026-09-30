from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(140, 180))
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0))) / F.count("*")), 1).alias("pct_basic"),
        F.max("end_date").alias("latest_end_date"),
    )
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .select(F.col("c.age"), F.col("sbc.pct_basic"), F.col("sbc.latest_end_date"))
)
