from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .groupBy("order_id")
    .pivot("carrier", ["Flash", "FedEx"])
    .agg(F.avg("cost"))
    .orderBy("order_id")
)
