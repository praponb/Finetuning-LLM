from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .groupBy(F.col("c.segment"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.plan") == "basic", 1).otherwise(0))) / F.count("*")), 1).alias("pct_basic"),
    )
    .filter(F.col("pct_basic") >= 49)
    .orderBy(F.col("segment").desc())
)
