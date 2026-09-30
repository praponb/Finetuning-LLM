from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("o.customer_id") == 15)
    .select(
        F.col("c.age"),
        F.col("c.is_active"),
        F.col("c.country"),
        F.col("o.order_date"),
    )
)
