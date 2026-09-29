from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.lower(F.col("c.name")).like("%liv%"))
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.end_date"),
        F.col("c.credit_limit"),
        F.col("c.age"),
        F.col("c.signup_date"),
    )
    .orderBy("signup_date")
)
