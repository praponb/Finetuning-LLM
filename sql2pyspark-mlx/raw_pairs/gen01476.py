from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter((F.upper(F.trim(F.col("c.name"))) == "PIM CHAIYAPORN") & F.col("c.signup_date").between("2023-03-14", "2025-06-18"))
    .select(
        F.col("s.id"),
        F.col("s.monthly_fee"),
        F.col("s.status"),
        F.col("s.plan"),
        F.col("c.age"),
    )
)
