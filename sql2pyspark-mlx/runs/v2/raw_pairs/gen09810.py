from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", "channel")
    .agg(
        F.min("amount").alias("min_amount"),
        F.sum("discount").alias("total_discount"),
        F.avg("discount").alias("avg_discount"),
    )
    .filter(F.col("min_amount") >= 1170)
    .orderBy("min_amount")
)
