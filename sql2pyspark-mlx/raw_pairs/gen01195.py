from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Flash")
    .groupBy("status")
    .pivot("carrier", ["Kerry", "Flash", "UPS", "FedEx"])
    .agg(F.avg("cost"))
    .orderBy("status")
)
