from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.city").isin(["London", "Chiang Mai"]))
    .select(
        F.col("orders.id"),
        F.col("orders.channel"),
        F.col("orders.status"),
        F.col("customers.name"),
    )
)
