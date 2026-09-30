from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([10, 29, 33, 35]))
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("shipping_fee").alias("shipping_fee_count"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
    .orderBy("status", F.col("shipping_fee_count").desc())
)
