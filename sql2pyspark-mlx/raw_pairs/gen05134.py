from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier", "order_id")
    .pivot("status", ["LOST", "RETURNED"])
    .agg(F.max("cost"))
)
