from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") > 35)
    .groupBy("status")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
    .filter(F.col("avg_amount") < 33)
    .select("status", "avg_amount", "cancelled_count")
    .orderBy(F.col("cancelled_count").desc(), "status")
    .limit(10)
)
