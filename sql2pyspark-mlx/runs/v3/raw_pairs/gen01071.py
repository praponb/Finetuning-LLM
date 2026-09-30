from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("o.status").isin(["CANCELLED", "PENDING", "SHIPPED"]))
    .groupBy(
        F.col("o.customer_id"),
        F.month(F.col("o.order_date")).alias("order_month"),
    )
    .agg(F.count("*").alias("total_count"))
)
