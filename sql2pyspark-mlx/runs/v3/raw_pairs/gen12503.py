from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") <= 49)
    .groupBy("carrier")
    .pivot("status", ["RETURNED", "DELIVERED", "IN_TRANSIT", "LOST"])
    .agg(F.avg("cost"))
)
