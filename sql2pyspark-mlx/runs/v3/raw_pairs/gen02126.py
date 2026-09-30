from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-05-01", "2025-04-01"))
    .groupBy("status", "carrier")
    .agg(F.max("cost").alias("max_cost"))
)
