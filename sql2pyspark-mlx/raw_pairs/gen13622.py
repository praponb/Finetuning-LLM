from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "DELIVERED"]) & (F.col("order_id") == 5))
)

summary = (
    recent_shipments
    .groupBy("status")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    summary
    .select("status", "avg_cost")
    .orderBy(F.col("status").desc(), F.col("avg_cost").desc())
    .limit(5)
)
