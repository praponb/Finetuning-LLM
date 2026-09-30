from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("channel").orderBy("amount", "id")
w2 = Window.partitionBy("channel").orderBy("seq_no", "id")
w3 = Window.partitionBy(F.year("order_date"), "channel").orderBy("discount_rounded")
w4 = Window.partitionBy("store_id").orderBy("prev_discount", "id")
w5 = Window.orderBy("prev_amount", "id")
w6 = Window.partitionBy("store_id")

valid = (
    df_orders
    .filter(F.col("order_date") >= F.to_date(F.lit("2023-06-09")))
    .select(
        "*",
        F.round("discount", 2).alias("discount_rounded"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.lead("amount").over(w_running).alias("next_amount"),
        F.lag("discount").over(w_running).alias("prev_discount"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "store_id",
        "shipping_fee",
        "channel_clean",
        "next_amount",
        "prev_discount",
        "seq_no",
        F.lag("amount", 1, 0).over(w2).alias("prev_amount"),
        F.dense_rank().over(w3).alias("discount_rounded_dense_rank"),
    )
)

scored_orders = (
    metrics
    .filter(F.col("store_id").isNull())
    .select(
        "id",
        "channel_clean",
        "next_amount",
        "seq_no",
        "prev_amount",
        F.lead("discount_rounded_dense_rank", 1).over(w4).alias("next_discount_rounded_dense_rank"),
        F.row_number().over(w5).alias("event_seq"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w6)), 4).alias("seq_no_share"),
    )
)

kept = (
    scored_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") <= "2025-08-01"), "left_anti")
)

result = (
    kept
    .groupBy("seq_no")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
    )
    .orderBy(F.col("event_seq_p50").desc_nulls_last())
)
