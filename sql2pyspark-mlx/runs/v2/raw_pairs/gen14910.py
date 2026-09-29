from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.customer_id").isin([11, 12]) & (F.col("o.amount") > 820))
    .select(
        F.col("s.order_id"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("c.age"),
        F.col("c.signup_date"),
    )
    .orderBy("signup_date")
)
