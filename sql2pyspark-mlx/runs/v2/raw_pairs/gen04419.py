from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.year(F.col("o.order_date")) == 2023) | F.col("c.is_active"))
    .select(
        F.col("p.method"),
        F.col("o.amount"),
        F.col("c.email"),
        F.col("c.is_active"),
    )
    .orderBy("method")
)
