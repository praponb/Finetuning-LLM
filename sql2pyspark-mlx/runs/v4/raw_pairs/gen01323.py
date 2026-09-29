from pyspark.sql import functions as F

base = df_orders.filter(F.col("discount").between(0.1, 0.26))

orders_stats = (
    base
    .groupBy("status")
    .agg(F.count("*").alias("cnt"))
)

result = (
    orders_stats
    .filter(F.col("cnt") < 10)
    .select("status", "cnt")
    .orderBy(F.col("cnt").desc(), "status")
    .limit(5)
)
