from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .pivot("carrier", ["UPS", "FedEx"])
    .agg(F.avg("cost"))
)
