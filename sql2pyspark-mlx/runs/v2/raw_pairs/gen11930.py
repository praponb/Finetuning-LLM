from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-11-03", "2025-08-07"))
    .groupBy("status")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
    .filter(F.col("stddev_cost") < 44)
    .select("status", "stddev_cost")
)
