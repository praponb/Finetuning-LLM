from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter((F.length(F.col("c.name")) < 14) | F.col("s.end_date").between("2024-02-01", "2024-10-01"))
    .select(F.col("s.end_date"), F.col("c.credit_limit"))
)
