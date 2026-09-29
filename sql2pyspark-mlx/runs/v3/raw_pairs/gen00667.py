from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(~((F.col("c.age") <= 42) & (F.col("o.order_date") < "2025-04-01")))
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("c.country"),
        F.col("c.age"),
        F.col("c.name"),
    )
)
