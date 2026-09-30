from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter((F.col("c.credit_limit") < 6300) & F.col("c.segment").isin(["SMB", "Consumer"]))
    .select(
        F.col("s.id"),
        F.col("s.monthly_fee"),
        F.col("s.plan"),
        F.col("s.start_date"),
        F.col("c.email"),
        F.col("c.name"),
    )
    .orderBy(F.col("id").desc())
)
