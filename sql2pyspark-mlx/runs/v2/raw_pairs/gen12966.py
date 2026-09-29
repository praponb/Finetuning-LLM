from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .select(
        F.col("c.city"),
        F.col("c.country"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("o.amount"),
    )
)
