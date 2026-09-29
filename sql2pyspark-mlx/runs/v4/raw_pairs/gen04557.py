from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .filter(F.col("stores.opened_date") < "2023-07-12")
    .select(
        F.col("orders.order_date"),
        F.col("orders.channel"),
        F.col("customers.name"),
        F.col("stores.opened_date"),
    )
    .orderBy("channel")
)
