from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy(F.col("discount").asc_nulls_first())
w4 = Window.partitionBy("method")
w_ordered = Window.partitionBy(F.date_trunc("MONTH", "paid_at"), "status").orderBy("order_date", "id")
w6 = Window.partitionBy("status", "order_id").orderBy(F.col("first_known_discount").desc(), "id")
w7 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w8 = Window.partitionBy("status").orderBy("shipping_fee")
w9 = Window.partitionBy("method", "order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

valid = (
    df_payments
    .filter((F.col("paid_at") > F.to_timestamp(F.lit("2023-09-23 16:00:00"))) & (~F.col("method").eqNullSafe(F.lit("promptpay"))))
    .select("*", F.upper("method").alias("method_upper"))
)

with_dims = (
    valid
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.*",
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        F.col("o.order_date"),
    )
)

with_windows = (
    with_dims
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "shipping_fee",
        "discount",
        F.last("amount", ignorenulls=True).over(w_running).alias("last_known_amount"),
        F.first("discount", ignorenulls=True).over(w2).alias("first_known_discount"),
        F.dense_rank().over(w3).alias("discount_dense_rank"),
        F.max("shipping_fee").over(w4).alias("method_max_shipping_fee"),
        F.lag("discount", 1).over(w_ordered).alias("prev_discount"),
    )
)

enriched = (
    with_windows
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.method",
        "src.amount",
        "src.paid_at",
        "src.status",
        "src.shipping_fee",
        "src.discount",
        "src.last_known_amount",
        "src.first_known_discount",
        "src.prev_discount",
        F.col("b.channel"),
        F.col("b.status").alias("order_status"),
        F.col("b.amount").alias("order_amount"),
    )
)

enriched_payments = (
    enriched
    .filter(F.col("shipping_fee") <= 19)
    .select(
        "id",
        "amount",
        "status",
        "shipping_fee",
        "last_known_amount",
        "channel",
        "order_status",
        "order_amount",
        F.row_number().over(w6).alias("row_seq"),
        F.avg("order_amount").over(w7).alias("order_amount_avg_90d"),
        F.round((F.col("last_known_amount") / F.sum("last_known_amount").over(w4)), 4).alias("last_known_amount_share"),
        F.cume_dist().over(w8).alias("shipping_fee_cume_dist"),
        F.avg("shipping_fee").over(w9).alias("shipping_fee_avg_90d"),
    )
)

filtered = (
    enriched_payments
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "amount",
        "shipping_fee",
        "last_known_amount",
        "channel",
        "order_status",
        "order_amount",
        "row_seq",
        "order_amount_avg_90d",
        "last_known_amount_share",
        "shipping_fee_cume_dist",
        "shipping_fee_avg_90d",
        F.greatest("last_known_amount", "last_known_amount_share").alias("greatest_last_known_amount_last_known_amount_share"),
        F.concat_ws("-", "status", "order_status").alias("status_order_status_key"),
    )
)

wide = (
    filtered
    .groupBy("channel")
    .pivot("order_status", ["CANCELLED", "COMPLETED"])
    .agg(F.avg("last_known_amount").alias("value_sum"))
)

result = (
    wide
    .select(
        "channel",
        "CANCELLED",
        "COMPLETED",
        F.when(F.col("channel").rlike("^(mobile|partner)$"), "OTHER_ZONE").when(F.col("channel").rlike("^(web|store)$"), "EXTENDED").otherwise("GROUP_A").alias("channel_zone"),
    )
)
