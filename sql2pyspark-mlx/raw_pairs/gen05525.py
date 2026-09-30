from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id").orderBy("amount_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("channel", "store_id").orderBy("amount_int", "id")
w3 = Window.partitionBy("channel").orderBy("discount", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("last3_sum_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy(F.col("amount_int").desc(), "id")
w6 = Window.partitionBy("channel", "customer_id").orderBy("shipping_fee", "id")
w7 = Window.partitionBy("customer_id", "channel")
w8 = Window.partitionBy("customer_id").orderBy(F.col("next_row_seq").desc(), "id")

filtered = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-07-04").cast("date"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "discount",
        "shipping_fee",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

metrics = (
    filtered
    .filter(F.col("store_id").isNull())
    .select(
        "id",
        "customer_id",
        "channel",
        "discount",
        "shipping_fee",
        "amount_int",
        F.sum("discount").over(w_grp).alias("running_discount"),
        F.lead("discount", 1).over(w_part).alias("next_discount"),
    )
)

enriched = (
    metrics
    .withColumn("last3_sum_discount", F.sum("discount").over(w3))
    .withColumn("last3_max_discount", F.max("discount").over(w3))
    .withColumn("last3_count_discount", F.count("discount").over(w3))
)

with_windows = (
    enriched
    .select(
        "id",
        "customer_id",
        "channel",
        "discount",
        "shipping_fee",
        "amount_int",
        "last3_count_discount",
        F.max("running_discount").over(w4).alias("running_max_running_discount"),
        F.sum("running_discount").over(w4).alias("running_sum_running_discount"),
        F.count("running_discount").over(w4).alias("running_count_running_discount"),
        F.avg("running_discount").over(w4).alias("running_avg_running_discount"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.lead("row_seq").over(w6).alias("next_row_seq"),
        F.lead("shipping_fee").over(w6).alias("next_shipping_fee"),
        F.lag("shipping_fee").over(w6).alias("prev_shipping_fee"),
        F.row_number().over(w6).alias("seq_no"),
        F.corr("discount", "shipping_fee").over(w7).alias("corr_discount_shipping_fee"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "customer_id",
        "channel",
        "discount",
        "amount_int",
        "last3_count_discount",
        "running_max_running_discount",
        "running_sum_running_discount",
        "next_shipping_fee",
        "prev_shipping_fee",
        "corr_discount_shipping_fee",
    )
)

result = (
    latest
    .groupBy("customer_id")
    .pivot("channel", ["store", "partner"])
    .agg(F.max("amount_int").alias("total"))
)
