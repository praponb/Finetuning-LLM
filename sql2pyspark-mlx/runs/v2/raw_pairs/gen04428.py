from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .pivot("status", ["RETURNED", "IN_TRANSIT"])
    .agg(F.avg("cost"))
)
