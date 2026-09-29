from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([14, 18, 31, 34]))
    .groupBy(
        "carrier",
        "status",
        F.when(F.col("status").isin(["DELIVERED", "RETURNED"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
    .orderBy("status", "carrier", "status_group")
    .limit(5)
)
