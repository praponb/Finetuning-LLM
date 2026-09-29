from pyspark.sql import functions as F

src = (
    df_orders
    .filter(F.col("channel") != "partner")
    .withColumn("clean_status", F.trim("status"))
)

agg = (
    src
    .groupBy("channel")
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.max("shipping_fee").alias("max_shipping_fee"),
    )
)

result = (
    agg
    .orderBy("channel", F.col("max_shipping_fee").desc())
    .limit(10)
)
