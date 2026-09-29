from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.alias("orders"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .groupBy(F.col("customers.id"), F.col("customers.country"))
    .agg(
        F.count(F.col("orders.id")).alias("order_count"),
        F.max(F.col("orders.order_date")).alias("latest_order_date"),
        F.count(F.col("orders.shipping_fee")).alias("shipping_fee_count"),
    )
    .filter(F.col("order_count") >= 3)
    .orderBy("id")
)
