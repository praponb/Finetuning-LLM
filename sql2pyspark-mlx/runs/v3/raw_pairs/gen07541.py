from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") <= 58)
    .groupBy("carrier")
    .pivot("status", ["RETURNED", "IN_TRANSIT", "LOST", "DELIVERED"])
    .agg(F.count("cost"))
    .orderBy("carrier")
)
