from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .pivot("status", ["PENDING", "RETURNED", "CANCELLED"])
    .agg(F.max("amount").alias("total"), F.count(F.lit(1)).alias("n"))
)
