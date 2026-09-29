from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .pivot("carrier", ["FedEx", "UPS", "DHL"])
    .agg(F.sum("cost"))
)
