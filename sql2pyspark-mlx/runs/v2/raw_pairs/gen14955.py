from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.col("c.city"))
    .agg(
        F.sum(F.col("s.monthly_fee")).alias("total_monthly_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.status")))).alias("status_values"),
    )
    .filter(F.col("total_monthly_fee") >= 740)
    .orderBy(F.col("city").desc(), F.col("status_values").desc())
)
