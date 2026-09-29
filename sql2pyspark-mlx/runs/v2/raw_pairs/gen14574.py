from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .select(
        F.col("c.country"),
        F.col("c.age"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
    )
)
