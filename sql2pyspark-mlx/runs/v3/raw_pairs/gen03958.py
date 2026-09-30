from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "right")
    .filter(F.col("c.city").isin(["Berlin", "Bangkok", "Tokyo"]))
    .groupBy(F.col("c.is_active"))
    .agg(
        F.sum(F.when(F.col("s.plan") == "standard", 1).otherwise(0)).alias("standard_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.status")))).alias("status_values"),
    )
)
