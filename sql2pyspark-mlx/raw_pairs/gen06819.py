from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("status", "customer_id").orderBy("running_avg_is_cancelled", "id")
w3 = Window.partitionBy("customer_id").orderBy("running_count_is_cancelled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id")

filtered = (
    df_orders
    .filter(F.col("store_id").isNotNull() & F.col("discount").isNotNull())
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.concat("status", F.lit(", "), "channel").alias("status_channel"),
        F.when(F.col("status") == "CANCELLED", 1).otherwise(0).alias("is_cancelled"),
    )
)

metrics = (
    filtered
    .filter(F.col("shipping_fee") >= 25)
    .withColumn("running_avg_is_cancelled", F.avg("is_cancelled").over(w))
    .withColumn("running_count_is_cancelled", F.count("is_cancelled").over(w))
    .withColumn("running_min_is_cancelled", F.min("is_cancelled").over(w))
)

with_windows = (
    metrics
    .select(
        "id",
        "customer_id",
        "status",
        "discount",
        "shipping_fee",
        "status_channel",
        "running_avg_is_cancelled",
        "running_count_is_cancelled",
        F.lead("running_min_is_cancelled").over(w_hist).alias("next_running_min_is_cancelled"),
        F.lag("shipping_fee").over(w_hist).alias("prev_shipping_fee"),
        F.lead("shipping_fee").over(w_hist).alias("next_shipping_fee"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)

with_windows_orders = (
    with_windows
    .select(
        "id",
        "customer_id",
        "discount",
        "shipping_fee",
        "status_channel",
        "next_running_min_is_cancelled",
        "next_shipping_fee",
        "seq_no",
        F.min("next_shipping_fee").over(w3).alias("running_min_next_shipping_fee"),
        F.max("next_shipping_fee").over(w3).alias("running_max_next_shipping_fee"),
        F.count("next_shipping_fee").over(w3).alias("running_count_next_shipping_fee"),
        F.avg("next_shipping_fee").over(w3).alias("running_avg_next_shipping_fee"),
        F.stddev_samp("prev_shipping_fee").over(w4).alias("customer_id_stddev_samp_prev_shipping_fee"),
    )
)

result = (
    with_windows_orders
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.country").alias("country"), F.col("d.segment").alias("segment"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("src.next_running_min_is_cancelled")).alias("total_next_running_min_is_cancelled"),
        F.grouping_id().alias("grouping_level"),
    )
)
