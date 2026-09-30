from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .pivot("status", ["IN_TRANSIT", "LOST", "RETURNED"])
    .agg(F.max("cost"))
    .orderBy("carrier")
)
