from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"), "status")
    .agg(F.avg("cost").alias("avg_cost"))
    .filter(F.col("avg_cost") < 65)
    .select("shipped_month", "status", "avg_cost")
)
