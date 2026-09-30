from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("orders.order_date")).alias("order_month"))
    .agg(F.max(F.col("shipments.cost")).alias("max_cost"))
    .filter(F.col("max_cost") > 89)
    .orderBy(F.col("max_cost").desc())
)
