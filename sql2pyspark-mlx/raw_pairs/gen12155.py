from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2023-08-19", "2024-06-01"))
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.count("discount").alias("discount_count"),
    )
    .filter(F.col("discount_count") < 7)
    .select("store_id", "stddev_discount", "discount_count")
)
