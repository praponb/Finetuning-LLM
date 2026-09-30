from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("shipping_fee") >= 22) & F.col("channel").isin(["mobile", "web"])) | (F.col("status") == "SHIPPED"))
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(
        F.sum(F.when(F.col("channel") == "store", 1).otherwise(0)).alias("store_count"),
        F.max("shipping_fee").alias("max_shipping_fee"),
    )
)
