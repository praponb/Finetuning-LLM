from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.channel"),
        F.col("c.name"),
        F.col("c.age"),
    )
    .orderBy("id")
)
