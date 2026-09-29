from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .pivot("status", ["LOST", "RETURNED", "IN_TRANSIT"])
    .agg(F.max("cost"))
    .orderBy("carrier")
)
