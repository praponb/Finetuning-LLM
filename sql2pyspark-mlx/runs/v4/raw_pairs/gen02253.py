from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "status")
    .agg(
        F.min("order_date").alias("min_order_date"),
        F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0)).alias("mobile_count"),
    )
    .orderBy("status")
)
