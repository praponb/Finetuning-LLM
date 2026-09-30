from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("subscription_plan", "channel").orderBy("discount", "id", "subscription_id", "subscription_id_2")
w2 = Window.partitionBy("plan").orderBy("amount_times_discount", "id", "subscription_id", "subscription_id_2").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("discount").desc(), "id", "subscription_id", "subscription_id_2")
w4 = Window.partitionBy(F.year("order_date"), "store_id")
w5 = Window.partitionBy("channel").orderBy("subscription_monthly_fee", "id", "subscription_id", "subscription_id_2")
w6 = Window.partitionBy("plan").orderBy(F.col("discount_bucket").desc_nulls_first())
w7 = Window.orderBy(F.col("discount_bucket").desc(), "id", "subscription_id", "subscription_id_2")

valid = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-09-15").cast("date"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "discount",
        "shipping_fee",
        (F.col("amount") * F.col("discount")).alias("amount_times_discount"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

matched = (
    valid
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.order_date") >= F.col("sub.start_date")) & (F.col("e.order_date") <= F.date_add(F.col("sub.end_date"), 3)), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

matched_orders = (
    matched
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.order_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

valid_orders = (
    matched_orders
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        (F.col("monthly_fee") - F.col("subscription_id")).alias("monthly_fee_minus_subscription_id"),
        F.coalesce("subscription_plan", F.lit("DIRECT")).alias("resolved_subscription_plan"),
    )
)

with_windows = (
    valid_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "discount",
        "shipping_fee",
        "subscription_id",
        "plan",
        "subscription_id_2",
        "subscription_monthly_fee",
        F.ntile(5).over(w).alias("discount_bucket"),
        F.avg("discount").over(w2).alias("discount_avg_2"),
        F.ntile(5).over(w3).alias("discount_bucket_2"),
        F.sum("shipping_fee").over(w4).alias("order_date_year_sum_shipping_fee"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "discount",
        "shipping_fee",
        "subscription_id",
        "subscription_id_2",
        "subscription_monthly_fee",
        "discount_avg_2",
        "order_date_year_sum_shipping_fee",
        F.rank().over(w6).alias("discount_bucket_rank"),
        F.row_number().over(w7).alias("seq_no"),
    )
)
