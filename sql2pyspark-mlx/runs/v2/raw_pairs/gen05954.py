from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "right")
    .groupBy(
        F.date_trunc("MONTH", F.col("orders.order_date")).alias("order_month"),
        F.col("orders.status"),
    )
    .agg(F.countDistinct(F.col("shipments.cost")).alias("unique_orders"))
    .orderBy("status")
)
