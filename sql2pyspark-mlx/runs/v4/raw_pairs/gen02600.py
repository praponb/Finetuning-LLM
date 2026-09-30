from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .pivot("carrier", ["DHL", "FedEx"])
    .agg(F.count("cost"))
    .orderBy("status")
)
