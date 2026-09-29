from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter(F.col("s.end_date").between("2023-03-07", "2024-03-14"))
    .select(
        F.col("s.start_date"),
        F.col("s.monthly_fee"),
        F.col("s.end_date"),
        F.col("c.credit_limit"),
        F.col("c.email"),
    )
)
