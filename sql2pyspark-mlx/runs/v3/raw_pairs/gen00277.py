from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.cost"),
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("cost").desc(), "signup_date")
)
