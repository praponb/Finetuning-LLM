from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .groupBy(F.col("c.id"), F.col("c.country"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.sum(F.when(F.col("s.plan") == "basic", 1).otherwise(0)).alias("basic_count"),
        F.min(F.col("s.end_date")).alias("first_end_date"),
    )
    .orderBy("id", "country")
    .limit(1)
)
