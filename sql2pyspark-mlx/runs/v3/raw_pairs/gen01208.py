from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.col("c.segment"), F.col("c.country"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
    )
    .orderBy(F.col("pct_premium").desc(), "segment", "country")
    .limit(1)
)
