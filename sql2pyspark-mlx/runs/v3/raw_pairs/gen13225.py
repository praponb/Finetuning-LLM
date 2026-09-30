from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .pivot("carrier", ["FedEx", "DHL"])
    .agg(F.max("cost"))
    .orderBy("status")
)
