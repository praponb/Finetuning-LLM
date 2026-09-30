from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("s.customer_id"),
        F.col("c.email"),
        F.col("c.segment"),
        F.col("c.country"),
    )
    .orderBy(F.col("customer_id").desc())
)
