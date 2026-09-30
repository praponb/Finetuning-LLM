from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .groupBy(F.col("c.country"))
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("s.plan") == "enterprise", 1)).alias("enterprise_count"),
    )
    .filter(F.col("enterprise_count") >= 1)
)
