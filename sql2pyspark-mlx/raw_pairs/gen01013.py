from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("o.customer_id").isin([21, 28]))
    .select(F.col("o.id"), F.col("o.shipping_fee"), F.col("c.email"), F.col("c.name"))
    .orderBy("name", "id")
    .limit(1)
)
