from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"), "channel")
    .agg(
        F.count("discount").alias("discount_count"),
        F.avg("discount").alias("avg_discount"),
    )
)
