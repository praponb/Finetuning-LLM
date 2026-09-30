from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .pivot("carrier", ["Flash", "Kerry", "FedEx"])
    .agg(F.sum("cost"))
)
