from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.status").isin(["COMPLETED", "SHIPPED"]) & F.col("o.amount").between(680, 1280))
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("o.order_date"),
        F.col("c.name"),
        F.col("c.email"),
    )
    .orderBy(F.col("order_id").desc())
)
