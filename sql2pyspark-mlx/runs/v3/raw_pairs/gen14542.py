from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2025-04-01")
    .groupBy("carrier")
    .pivot("status", ["RETURNED", "IN_TRANSIT"])
    .agg(F.avg("cost"))
    .orderBy("carrier")
)
