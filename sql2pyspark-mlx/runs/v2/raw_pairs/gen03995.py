from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.filter(F.col("quantity").between(3, 5)).alias("order_items"), F.col("orders.id") == F.col("order_items.order_id"), "left_anti")
    .filter(F.col("orders.amount") < 1370)
    .select(
        F.col("orders.id"),
        F.col("orders.discount"),
        F.col("orders.order_date"),
        F.col("orders.customer_id"),
    )
    .orderBy(F.col("id").desc())
)
