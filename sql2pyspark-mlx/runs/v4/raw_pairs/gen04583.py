from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.status") == "COMPLETED")
    .groupBy(F.col("o.status"))
    .agg(
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
        F.count(F.when(F.col("s.carrier") == "UPS", 1)).alias("ups_count"),
    )
    .orderBy(F.col("avg_cost").desc(), "status")
    .limit(20)
)
