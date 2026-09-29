from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .pivot("carrier", ["FedEx", "Flash"])
    .agg(F.sum("cost"))
)
