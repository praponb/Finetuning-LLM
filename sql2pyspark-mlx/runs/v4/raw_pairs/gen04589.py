from pyspark.sql import functions as F

src = df_orders.filter(F.col("discount") < 0.26)

orders_stats = (
    src
    .groupBy("status")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    orders_stats
    .filter(F.col("avg_amount") < 29)
    .select("status", "avg_amount", "max_amount")
    .orderBy("status", F.col("max_amount").desc())
)
