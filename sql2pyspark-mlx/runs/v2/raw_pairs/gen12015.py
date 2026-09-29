from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.country") == "SG")
    .groupBy(F.col("c.is_active"))
    .agg(F.sort_array(F.collect_set(F.col("s.plan"))).alias("plan_list"))
)
