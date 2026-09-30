from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_month"))
    .agg(
        F.sum(F.col("s.cost")).alias("total_cost"),
        F.sum(F.when(F.col("s.status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
    )
)
