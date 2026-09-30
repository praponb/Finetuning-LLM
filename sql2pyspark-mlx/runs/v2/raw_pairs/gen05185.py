from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.email").like("%@company.co"))
    .groupBy(F.col("c.country"), F.col("c.is_active"))
    .agg(
        F.sort_array(F.collect_set(F.col("s.plan"))).alias("plan_list"),
        F.sum(F.col("s.monthly_fee")).alias("total_monthly_fee"),
    )
    .orderBy(F.col("total_monthly_fee").desc(), "country", "is_active")
    .limit(20)
)
