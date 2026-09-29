from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "right")
    .filter((F.col("s.plan") != "premium") & F.month(F.col("s.start_date")).isin([1, 2, 9]))
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("s.end_date"),
        F.col("c.segment"),
        F.col("c.country"),
        F.col("c.credit_limit"),
    )
)
