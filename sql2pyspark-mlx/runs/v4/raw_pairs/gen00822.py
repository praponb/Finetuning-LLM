from pyspark.sql import functions as F

src = df_orders.filter(F.col("channel") != "store")

orders_stats = (
    src
    .groupBy("channel")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    orders_stats
    .filter(F.col("avg_amount") < 52)
    .orderBy(F.col("avg_amount").desc())
)
