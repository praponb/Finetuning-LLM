from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.count("amount").alias("amount_count"),
        F.avg("discount").alias("avg_discount"),
        F.max("discount").alias("max_discount"),
    )
    .filter(F.col("max_discount") > 0.22)
    .drop("max_discount")
    .orderBy("status")
)
