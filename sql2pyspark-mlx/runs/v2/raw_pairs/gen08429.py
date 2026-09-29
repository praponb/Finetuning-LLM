from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .pivot("status", ["LOST", "IN_TRANSIT", "RETURNED", "DELIVERED"])
    .agg(F.sum("cost"))
    .orderBy("carrier")
)
