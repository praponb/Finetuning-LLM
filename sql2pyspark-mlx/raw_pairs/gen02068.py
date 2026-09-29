from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("order_date").desc(), "id")

latest = (
    df_orders
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .groupBy(
        "channel",
        F.date_trunc("month", "order_date").alias("activity_month"),
        F.when(F.col("discount") >= 0.38, "HIGH").otherwise("LOW").alias("discount_tier"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "web", F.col("discount")).otherwise(0)).alias("web_discount"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.max(F.when(F.col("channel") == "mobile", F.col("shipping_fee"))).alias("mobile_max_shipping_fee"),
        F.stddev_samp("discount").alias("discount_stddev"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy("channel")
)
