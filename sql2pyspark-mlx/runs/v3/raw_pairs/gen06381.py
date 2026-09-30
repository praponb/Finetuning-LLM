from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .pivot("carrier", ["UPS", "Flash", "FedEx"])
    .agg(F.count("cost"))
)
