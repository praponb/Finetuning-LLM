from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age") <= 67)
    .select(
        F.col("s.id"),
        F.col("s.end_date"),
        F.col("c.name"),
        F.col("c.city"),
        F.col("c.email"),
    )
    .orderBy(F.col("email").desc(), "id")
)
