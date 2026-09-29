from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.sum(F.col("o.discount")).alias("total_discount"),
    )
)
