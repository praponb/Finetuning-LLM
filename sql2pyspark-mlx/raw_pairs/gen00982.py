from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"), "status")
    .agg(
        F.countDistinct("shipping_fee").alias("unique_channels"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
    .orderBy(F.col("order_month").desc())
)
