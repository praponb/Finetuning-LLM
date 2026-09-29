from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .pivot("carrier", ["Flash", "FedEx", "UPS"])
    .agg(F.max("cost"))
    .orderBy("status")
)
