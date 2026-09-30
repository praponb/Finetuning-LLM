from pyspark.sql import functions as F

src = (
    df_orders
    .filter(F.col("discount").between(0.15, 0.24))
    .withColumn("status_title", F.initcap(F.trim("status")))
)

agg = (
    src
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
)

result = agg.orderBy(F.col("mobile_count").desc())
