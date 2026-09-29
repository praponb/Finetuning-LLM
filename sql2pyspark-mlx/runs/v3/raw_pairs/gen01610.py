from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.month("shipped_date").isin([3, 5, 10]))

agg = (
    recent_shipments
    .groupBy("status")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    agg
    .filter(F.col("avg_cost") >= 79)
    .select("status", "avg_cost")
    .orderBy(F.col("status").desc())
)
