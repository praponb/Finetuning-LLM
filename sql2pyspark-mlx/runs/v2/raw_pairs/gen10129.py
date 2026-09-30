from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .pivot("carrier", ["Kerry", "DHL"])
    .agg(F.avg("cost"))
    .orderBy("status")
)
