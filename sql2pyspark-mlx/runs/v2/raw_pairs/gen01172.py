from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.segment").isin(["Consumer", "Corporate", "SMB"]) & (F.col("subscriptions.monthly_fee") < 230))
    .select(
        F.col("subscriptions.start_date"),
        F.col("customers.city"),
        F.col("customers.country"),
        F.col("customers.email"),
        (F.col("customers.credit_limit") - F.col("customers.age")).alias("credit_limit_minus_age"),
    )
    .orderBy(F.col("country").desc())
)
