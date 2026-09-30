from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([6, 8, 9]))
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("shipped_month"), "status")
    .agg(F.avg("cost").alias("avg_cost"))
    .orderBy(F.col("status").desc(), "shipped_month")
    .limit(3)
)
