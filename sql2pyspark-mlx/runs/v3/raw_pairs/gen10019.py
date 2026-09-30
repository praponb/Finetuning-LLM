from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter((F.col("o.channel") == "mobile") & F.col("o.shipping_fee").between(21, 32))
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
        F.col("c.email"),
    )
    .orderBy(F.col("store_id").desc(), F.col("order_date").desc())
)
