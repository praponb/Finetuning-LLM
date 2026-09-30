from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .filter(F.col("s.status").isin(["LOST", "RETURNED"]))
    .groupBy(
        F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_month"),
        F.col("o.status"),
    )
    .agg(
        F.sum(F.when(F.col("s.status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
        F.max(F.col("s.cost")).alias("max_cost"),
    )
    .orderBy(F.col("status").desc(), "order_month")
    .limit(10)
)
