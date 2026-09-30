from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "DELIVERED", "LOST"]))
    .groupBy("carrier")
    .pivot("status", ["RETURNED", "IN_TRANSIT", "DELIVERED"])
    .agg(F.max("cost"))
)
