from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .pivot("status", ["DELIVERED", "LOST", "RETURNED"])
    .agg(F.max("cost"))
    .orderBy("carrier")
)
