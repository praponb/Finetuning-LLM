from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.cost"),
        F.col("o.shipping_fee"),
        F.col("c.age"),
        F.col("c.credit_limit"),
        F.col("s.cost").cast("int").alias("cost_int"),
    )
)
