from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([1, 14, 34]))
    .groupBy("status")
    .pivot("carrier", ["Flash", "FedEx", "Kerry"])
    .agg(F.avg("cost"))
)
