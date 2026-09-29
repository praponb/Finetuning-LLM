from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("stddev_discount") >= 40)
    .orderBy(F.col("customer_id").desc())
)
