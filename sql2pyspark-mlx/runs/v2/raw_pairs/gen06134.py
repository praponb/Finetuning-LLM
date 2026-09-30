from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("c.country").isin(["DE", "TH"]) & F.col("c.is_active"))
    .select(
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("o.amount"),
        F.col("c.email"),
        F.col("c.signup_date"),
    )
    .orderBy(F.col("amount").desc())
)
