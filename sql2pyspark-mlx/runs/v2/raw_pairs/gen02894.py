from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .groupBy(F.col("c.segment"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        (F.max(F.col("o.discount")) - F.min(F.col("o.discount"))).alias("discount_range"),
        F.max(F.col("o.order_date")).alias("latest_order_date"),
    )
    .orderBy("latest_order_date")
)
