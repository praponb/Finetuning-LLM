from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .pivot("status", ["DELIVERED", "IN_TRANSIT", "RETURNED", "LOST"])
    .agg(F.sum("cost"))
)
