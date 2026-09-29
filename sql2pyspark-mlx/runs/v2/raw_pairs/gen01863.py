from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        F.countDistinct(F.col("s.monthly_fee")).alias("unique_customers"),
        F.min(F.col("s.end_date")).alias("first_end_date"),
    )
    .filter(F.col("unique_customers") > 1)
    .orderBy(F.col("segment").desc(), "unique_customers")
    .limit(50)
)
