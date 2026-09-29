from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "right")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.date_format(F.col("orders.order_date"), "yyyy-MM").alias("order_month"))
    .agg(
        F.sum(F.when(F.col("shipments.carrier") == "UPS", 1).otherwise(0)).alias("ups_count"),
        F.countDistinct(F.col("shipments.cost")).alias("unique_orders"),
    )
    .orderBy("order_month", F.col("ups_count").desc())
)
