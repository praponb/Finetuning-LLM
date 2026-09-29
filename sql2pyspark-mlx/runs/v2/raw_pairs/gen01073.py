from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") > "2025-03-01") & F.month("shipped_date").isin([1, 8, 10]) & F.col("cost").between(59, 64))
    .groupBy("status", "carrier")
    .agg(F.avg("cost").alias("avg_cost"))
)
