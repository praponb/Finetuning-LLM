from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(F.col("discount") >= 0.12)
    .withColumn("channel_upper", F.upper("channel"))
)

shipping_fee_bucket_summary = (
    valid_orders
    .groupBy(
        F.when(F.col("shipping_fee") >= 44, "high").when(F.col("shipping_fee") >= 7, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.min("order_date").alias("first_order_date"))
)

result = shipping_fee_bucket_summary
