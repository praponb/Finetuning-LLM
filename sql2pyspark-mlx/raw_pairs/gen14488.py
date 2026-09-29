from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("customer_id", "status").orderBy("discount", "id")

eligible = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") < F.col("s.order_date")), "left_anti")
)

base = (
    eligible
    .filter(F.col("customer_id").isNotNull() & (F.col("shipping_fee") < 21))
    .select(
        "*",
        F.when(F.col("status") == "COMPLETED", 1).otherwise(0).alias("is_completed"),
    )
)

eligible_orders = (
    base
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.carrier").isin(["DHL", "Flash", "Kerry"])), "left_anti")
)

scored = (
    eligible_orders
    .select(
        "*",
        F.stddev_samp("shipping_fee").over(w).alias("channel_stddev_samp_shipping_fee"),
        F.lag("is_completed", 1).over(w2).alias("prev_is_completed"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("prev_is_completed") >= 83, "LARGE").otherwise("SMALL").alias("prev_is_completed_tier"),
        F.when(F.col("shipping_fee") >= 38, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.round(F.avg("channel_stddev_samp_shipping_fee"), 2).alias("avg_channel_stddev_samp_shipping_fee"),
        F.sum(F.when(F.col("channel") == "web", F.col("discount")).otherwise(0)).alias("web_discount"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_rows"),
    )
    .orderBy("store_rows")
)
