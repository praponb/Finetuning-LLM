from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_status", "method").orderBy("amount", "id")

filtered = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.col("method").isin(["promptpay", "card", "cash"]))
    .select(
        "*",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
        F.coalesce("method", F.lit("NONE")).alias("method_filled"),
    )
)

enriched = (
    filtered
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.store_id") == 37)
    .select(
        "a.*",
        F.col("o.shipping_fee"),
        F.col("o.status").alias("order_status"),
        F.col("o.channel"),
    )
)

cleaned = (
    enriched
    .filter(F.col("order_id").isNotNull() & (F.col("amount") > 1360))
    .withColumn("is_success", F.when(F.col("status") == "SUCCESS", 1).otherwise(0))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("NONE")))
)

joined = (
    cleaned
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.method",
        "a.amount",
        "a.days_since_paid_at",
        "a.method_filled",
        "a.order_status",
        "a.channel",
        "a.is_success",
        "a.resolved_channel",
        F.col("b.shipping_fee").alias("order_shipping_fee"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "method",
        "method_filled",
        "order_status",
        F.lag("is_success").over(w).alias("prev_is_success"),
        F.lead("is_success").over(w).alias("next_is_success"),
        F.lead("order_shipping_fee").over(w).alias("next_order_shipping_fee"),
        F.row_number().over(w).alias("event_seq"),
    )
)

result = (
    with_windows
    .rollup("order_status")
    .agg(
        F.sort_array(F.collect_set("order_status")).alias("order_status_list"),
        F.percentile_approx("next_order_shipping_fee", [0.25, 0.5, 0.75]).alias("next_order_shipping_fee_quartiles"),
    )
    .orderBy(F.col("order_status").asc_nulls_first())
)
