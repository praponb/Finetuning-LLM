from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"))
    .pivot("status", ["DELIVERED", "LOST", "IN_TRANSIT"])
    .agg(F.count("cost"))
    .orderBy("shipped_quarter")
)
