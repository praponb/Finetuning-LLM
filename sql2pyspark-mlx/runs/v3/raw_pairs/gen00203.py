from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.country").isin(["UK", "DE"]) & (F.col("c.country") == "SG"))
    .select(
        F.col("s.plan"),
        F.col("s.monthly_fee"),
        F.col("s.status"),
        F.col("c.credit_limit"),
        F.col("c.city"),
    )
)
