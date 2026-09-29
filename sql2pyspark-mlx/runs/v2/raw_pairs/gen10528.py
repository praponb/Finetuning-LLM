from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(F.col("customer_id") == 35)
    .withColumn("shipping_fee_tier", F.when(F.col("shipping_fee") >= 39, "High").when(F.col("shipping_fee") >= 15, "Medium").otherwise("Low"))
)

agg = (
    recent_orders
    .groupBy("channel", "status")
    .agg(F.round(F.stddev("discount"), 2).alias("stddev_discount"))
)

result = (
    agg
    .select("channel", "status", "stddev_discount")
    .orderBy(F.col("status").desc())
)
