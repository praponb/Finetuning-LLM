from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("order_amount", "id")
w2 = Window.partitionBy("status").orderBy("least_next_order_amount_order_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("next_order_amount").desc(), "id")
w4 = Window.partitionBy("channel", "status")
w5 = Window.partitionBy("status", "channel").orderBy("running_event_seq", "id")
w6 = Window.partitionBy("status", "channel").orderBy(F.col("least_next_order_amount_order_amount").desc(), "id")

base = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("method", F.lit("OTHER")).alias("method_clean"),
    )
)

with_dims = (
    base
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id").eqNullSafe(F.col("d.id")), "left")
    .select("e.*", F.col("d.channel"), F.col("d.amount").alias("order_amount"))
)

with_windows = (
    with_dims
    .filter(F.to_date("paid_at") >= "2025-07-28")
    .select(
        "id",
        "status",
        "channel",
        "order_amount",
        F.lead("amount_int").over(w).alias("next_amount_int"),
        F.lag("order_amount").over(w).alias("prev_order_amount"),
        F.lead("order_amount").over(w).alias("next_order_amount"),
        F.row_number().over(w).alias("event_seq"),
    )
)

valid = (
    with_windows
    .filter(F.col("channel").isNotNull() & (F.col("channel") == "store"))
    .select(
        "id",
        "status",
        "channel",
        "order_amount",
        "prev_order_amount",
        "next_order_amount",
        "event_seq",
        (F.col("prev_order_amount") + F.col("order_amount")).alias("prev_order_amount_plus_order_amount"),
        F.least("next_order_amount", "order_amount").alias("least_next_order_amount_order_amount"),
    )
)

scored = (
    valid
    .withColumn("running_event_seq", F.sum("event_seq").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("channel_stddev_samp_least_next_order_amount_order_amount", F.stddev_samp("least_next_order_amount_order_amount").over(w4))
)

with_windows_payments = (
    scored
    .select(
        "id",
        "channel",
        "next_order_amount",
        "prev_order_amount_plus_order_amount",
        "running_event_seq",
        F.ntile(5).over(w5).alias("running_event_seq_bucket"),
        F.row_number().over(w6).alias("seq_no_2"),
    )
)

cleaned = (
    with_windows_payments
    .filter(F.col("channel").isNotNull() & (F.col("seq_no_2") == 4))
    .select(
        "*",
        F.coalesce("channel", F.lit("unassigned")).alias("channel_filled"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

result = (
    cleaned
    .withColumn("seq_no_2_tier", F.when(F.col("seq_no_2") >= 7, "LARGE").otherwise("SMALL"))
    .withColumn("next_order_amount_tier", F.when(F.col("next_order_amount") >= 1690, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["seq_no_2_tier", "next_order_amount_tier"], ["seq_no_2_tier"], []],
        "seq_no_2_tier",
        "next_order_amount_tier",
    )
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_rows"),
        F.round(F.sum("running_event_seq_bucket") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("running_event_seq_bucket_per_store"),
    )
    .orderBy(F.col("next_order_amount_tier").desc_nulls_first())
)
