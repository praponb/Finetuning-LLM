from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("channel", "customer_id").orderBy("discount", "id")
w3 = Window.partitionBy("customer_id").orderBy("discount", "id")
w_ordered = Window.orderBy(F.col("discount").desc(), "id")
w5 = Window.partitionBy("customer_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id", "payment_status").orderBy(F.col("prev_discount").asc_nulls_first())
w7 = Window.partitionBy("channel").orderBy("discount", "id")
w8 = Window.partitionBy("customer_id", "channel").orderBy("seq_no", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy("customer_id").orderBy("event_seq_3", "id").rowsBetween(-5, -1)
w10 = Window.partitionBy("customer_id").orderBy(F.col("last4_sum_prev_discount").desc_nulls_last(), "id")

joined = (
    df_orders
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.country"))
)

with_last = (
    joined
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.channel",
        "s.discount",
        F.col("c.status").alias("payment_status"),
    )
)

enriched = (
    with_last
    .select(
        "id",
        "customer_id",
        "channel",
        "discount",
        "payment_status",
        F.lag("discount").over(w2).alias("prev_discount"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("event_seq_2"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.quantity") >= 6), "left_anti")
)

metrics = (
    kept
    .filter(F.col("discount").isNotNull())
    .select(
        "*",
        F.row_number().over(w_ordered).alias("seq_no"),
        F.first("channel", ignorenulls=True).over(w5).alias("first_known_channel"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.cume_dist().over(w6).alias("prev_discount_cume_dist"),
        F.row_number().over(w7).alias("event_seq_3"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "customer_id",
        "payment_status",
        "prev_discount",
        "event_seq_2",
        "first_known_channel",
        "row_seq",
        "prev_discount_cume_dist",
        F.max("prev_discount").over(w8).alias("last4_max_prev_discount"),
        F.min("prev_discount").over(w8).alias("last4_min_prev_discount"),
        F.sum("prev_discount").over(w8).alias("last4_sum_prev_discount"),
        F.avg("event_seq_3").over(w9).alias("event_seq_3_avg_5_prior"),
    )
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .withColumn("last4_sum_prev_discount_tier", F.when(F.col("last4_sum_prev_discount") >= 2.19, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["last4_sum_prev_discount_tier", "customer_id"], ["last4_sum_prev_discount_tier"]],
        "last4_sum_prev_discount_tier",
        "customer_id",
    )
    .agg(
        F.count(F.when(F.col("payment_status") == "SUCCESS", 1)).alias("success_rows"),
        F.round(F.sum("event_seq_2") / F.nullif(F.count(F.when(F.col("payment_status") == "SUCCESS", 1)), F.lit(0)), 2).alias("event_seq_2_per_success"),
        F.percentile_approx("prev_discount", 0.9).alias("prev_discount_p90"),
        F.count("*").alias("event_count"),
    )
)
