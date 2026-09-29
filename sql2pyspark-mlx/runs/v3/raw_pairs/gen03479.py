from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.name").like("Dav%") | (F.col("c.signup_date") <= "2023-07-01"))
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("c.email"),
        F.col("c.age"),
        F.col("c.signup_date"),
        F.lower(F.col("c.segment")).alias("segment_lower"),
    )
    .orderBy("email")
)
