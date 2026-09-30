from pyspark.sql import functions as F

filtered_orders = df_orders.filter(F.col("customer_id") == 31)

agg = (
    filtered_orders
    .groupBy("channel")
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    agg
    .orderBy(F.col("channel").desc())
    .limit(5)
)
