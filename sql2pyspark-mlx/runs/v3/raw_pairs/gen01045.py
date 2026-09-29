from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.status").isin(["PENDING", "COMPLETED", "SHIPPED"]))
    .groupBy(F.col("o.status"))
    .agg(
        F.sum(F.col("s.cost")).alias("total_cost"),
        F.sum(F.when(F.col("s.status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
    )
    .filter(F.col("total_cost") >= 530)
    .orderBy("total_cost")
)
