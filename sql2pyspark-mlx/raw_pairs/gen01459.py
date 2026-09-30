from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(~((F.col("c.credit_limit") > 9400) & F.col("o.channel").isin(["web", "partner"])))
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        F.col("c.email"),
        F.col("c.is_active"),
        F.col("c.signup_date"),
    )
)
