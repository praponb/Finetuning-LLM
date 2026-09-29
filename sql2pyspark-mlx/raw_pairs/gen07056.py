from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.name").like("%Diaz%") & F.col("o.discount").between(0.11, 0.13))
    .select(
        F.col("c.country"),
        F.col("c.signup_date"),
        F.col("c.city"),
        F.col("o.order_date"),
    )
    .orderBy("country", "order_date")
)
