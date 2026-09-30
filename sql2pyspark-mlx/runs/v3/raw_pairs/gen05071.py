from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0)).alias("expired_count"),
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee")).otherwise(0)).alias("basic_monthly_fee"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.basic_monthly_fee") > 45)
    .select(
        F.col("customers.id"),
        F.col("customers.email"),
        F.col("customers.age"),
        F.col("t.expired_count"),
        F.col("t.basic_monthly_fee"),
    )
    .orderBy(F.col("basic_monthly_fee").desc(), "id")
    .limit(1)
)
