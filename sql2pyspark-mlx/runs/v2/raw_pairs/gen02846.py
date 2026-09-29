from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "FedEx")
    .groupBy("carrier")
    .pivot("status", ["DELIVERED", "RETURNED", "IN_TRANSIT", "LOST"])
    .agg(F.avg("cost"))
)
