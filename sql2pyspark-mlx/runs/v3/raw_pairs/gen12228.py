from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.is_active"))
    .groupBy(F.col("c.segment"))
    .agg(
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
        F.sum(F.when(F.col("s.status") == "DELIVERED", F.col("s.cost")).otherwise(0)).alias("delivered_cost"),
    )
)
