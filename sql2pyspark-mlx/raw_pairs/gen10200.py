from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .pivot("carrier", ["UPS", "DHL"])
    .agg(F.avg("cost"))
    .orderBy("status")
)
