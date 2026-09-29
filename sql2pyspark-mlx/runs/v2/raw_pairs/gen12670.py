from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .pivot("carrier", ["DHL", "Flash", "FedEx", "UPS"])
    .agg(F.sum("cost"))
)
