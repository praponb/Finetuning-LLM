from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .groupBy("carrier")
    .pivot("status", ["DELIVERED", "RETURNED"])
    .agg(F.avg("cost"))
)
