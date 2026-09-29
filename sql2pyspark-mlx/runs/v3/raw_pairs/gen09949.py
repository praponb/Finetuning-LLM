from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter((F.col("c.segment") == "Consumer") & (F.col("o.amount") <= 790))
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("c.city"),
    )
    .orderBy("id")
)
