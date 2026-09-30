from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.avg("discount").alias("avg_discount"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
    )
    .orderBy(F.col("order_quarter").desc())
)
