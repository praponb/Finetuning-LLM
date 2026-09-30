from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "Flash", "Kerry"]))
    .groupBy("status")
    .pivot("carrier", ["DHL", "Flash", "FedEx", "Kerry"])
    .agg(F.avg("cost"))
)
