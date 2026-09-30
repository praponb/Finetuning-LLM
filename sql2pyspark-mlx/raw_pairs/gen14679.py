from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("order_date"), "status").orderBy("amount", "id")
w2 = Window.orderBy("order_date", "id")

valid = (
    df_orders
    .filter(~F.col("channel").eqNullSafe(F.lit("mobile")))
    .select(
        "*",
        F.concat_ws("-", "status", "channel").alias("status_channel_key"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") < F.col("s.order_date")), "left_anti")
)

with_windows = (
    not_flagged
    .filter(F.col("discount") < 0.27)
    .select(
        "id",
        "order_date",
        "channel",
        "shipping_fee",
        "resolved_channel",
        F.row_number().over(w).alias("row_seq"),
        F.lag("discount", 1, 0).over(w2).alias("prev_discount"),
    )
)

filtered = (
    with_windows
    .filter(F.col("order_date") >= F.lit("2023-05-09").cast("date"))
    .select(
        "id",
        "order_date",
        "shipping_fee",
        "resolved_channel",
        "row_seq",
        "prev_discount",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel_2"),
    )
)

result = (
    filtered
    .groupBy(
        F.date_trunc("WEEK", "order_date").alias("week_start"),
        F.when(F.col("shipping_fee") >= 45, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
    )
    .agg(
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.percentile_approx("prev_discount", [0.25, 0.5, 0.75]).alias("prev_discount_quartiles"),
        F.min("days_since_order_date").alias("min_days_since_order_date"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("min_days_since_order_date") > 0)
    .orderBy(F.col("shipping_fee_tier").asc_nulls_first())
)
