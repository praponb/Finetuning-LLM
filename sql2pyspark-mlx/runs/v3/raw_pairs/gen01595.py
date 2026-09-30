from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.segment"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.min(F.col("o.order_date")).alias("min_order_date"),
    )
)
