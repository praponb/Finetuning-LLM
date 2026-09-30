from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .pivot("carrier", ["FedEx", "Flash", "DHL"])
    .agg(F.avg("cost"))
)
