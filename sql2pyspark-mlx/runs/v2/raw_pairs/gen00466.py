from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .pivot("carrier", ["Flash", "UPS", "Kerry", "FedEx"])
    .agg(F.count("cost"))
    .orderBy("status")
)
