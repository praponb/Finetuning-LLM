from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter((F.col("s.customer_id") == 7) & F.col("c.credit_limit").between(10500, 13200))
    .select(
        F.col("s.id"),
        F.col("s.plan"),
        F.col("s.monthly_fee"),
        F.col("s.customer_id"),
        F.col("c.age"),
        F.round(F.col("s.monthly_fee"), 2).alias("monthly_fee_rounded"),
    )
    .orderBy(F.col("customer_id").desc())
)
