from pyspark.sql import functions as F

src = df_orders.filter(F.col("store_id").isNotNull())

summary = (
    src
    .groupBy("channel")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
)

result = (
    summary
    .filter(F.col("stddev_discount") > 67)
    .orderBy("channel")
)
