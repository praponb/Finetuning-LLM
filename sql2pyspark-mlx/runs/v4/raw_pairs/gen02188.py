from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(F.avg("cost").alias("avg_cost"))
    .orderBy(F.col("shipped_quarter").desc())
    .limit(5)
)
