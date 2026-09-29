from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.end_date").between("2023-04-01", "2024-12-19"))
    .select(
        F.col("s.customer_id"),
        F.col("s.start_date"),
        F.col("s.plan"),
        F.col("c.signup_date"),
        F.col("c.age"),
        F.col("c.city"),
    )
    .orderBy("signup_date")
)
