from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("p.amount").between(550, 1280) & F.col("o.customer_id").isNotNull())
    .select(
        F.col("p.order_id"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("c.segment"),
        F.col("c.country"),
    )
)
