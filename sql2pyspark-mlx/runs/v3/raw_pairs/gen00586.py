from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .groupBy(F.col("c.id"), F.col("c.segment"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.round(F.avg(F.col("o.discount")), 2).alias("avg_discount"),
        F.min(F.col("o.order_date")).alias("min_order_date"),
    )
)
