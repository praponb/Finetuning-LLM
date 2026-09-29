from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .groupBy(F.col("c.id"), F.col("c.name"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.max(F.col("o.order_date")).alias("last_order_date"),
        F.max(F.col("o.order_date")).alias("latest_order_date"),
    )
    .orderBy("name")
)
