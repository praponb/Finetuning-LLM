from pyspark.sql import functions as F

recent_orders = df_orders.filter((F.col("shipping_fee") < 12) & F.col("amount").between(990, 1000))

orders_stats = (
    recent_orders
    .groupBy("status")
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
    )
)

result = (
    orders_stats
    .orderBy(F.col("status").desc())
    .limit(3)
)
