from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc())
w2 = Window.partitionBy("order_id", "method")
w_part = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.to_date("paid_at"), "method")
w5 = Window.partitionBy(F.date_trunc("WEEK", "paid_at")).orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("status").orderBy("amount_share", "id")
w7 = Window.partitionBy("status").orderBy("seq_no", "id")
w8 = Window.orderBy(F.col("next_amount").desc(), "id")
w9 = Window.orderBy("event_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("seq_no", "id")
w11 = Window.partitionBy(F.to_date("paid_at")).orderBy("next_amount", "id").rowsBetween(-6, Window.currentRow)
w12 = Window.orderBy("next_prev_amount", "id")
w13 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.orderBy("paid_at_to_date_avg_amount", "id")
w15 = Window.orderBy("event_seq_3", "id")
w16 = Window.partitionBy(F.month("paid_at")).orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_payments
    .select(
        "*",
        F.rank().over(w).alias("amount_rank"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.last("method", ignorenulls=True).over(w_part).alias("last_known_method"),
        F.avg("amount").over(w4).alias("paid_at_to_date_avg_amount"),
        F.sum("amount").over(w5).alias("running_amount"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "paid_at",
        "status",
        "amount_rank",
        "amount_share",
        "paid_at_to_date_avg_amount",
        F.lead("amount").over(w_running).alias("next_amount"),
        F.lag("amount").over(w_running).alias("prev_amount"),
        F.lead("amount_share").over(w_running).alias("next_amount_share"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)

with_windows_payments = (
    enriched
    .filter(F.col("seq_no") <= 6)
    .select(
        "id",
        "paid_at",
        "amount_share",
        "paid_at_to_date_avg_amount",
        "next_amount",
        "prev_amount",
        "seq_no",
        F.lead("seq_no").over(w7).alias("next_seq_no"),
        F.lag("seq_no").over(w7).alias("prev_seq_no"),
        F.lead("prev_amount").over(w7).alias("next_prev_amount"),
        F.row_number().over(w7).alias("event_seq"),
        F.row_number().over(w8).alias("event_seq_2"),
    )
)

enriched_payments = (
    with_windows_payments
    .select(
        "*",
        F.last("prev_amount", ignorenulls=True).over(w9).alias("last_known_prev_amount"),
        F.row_number().over(w10).alias("event_seq_3"),
        F.avg("prev_seq_no").over(w11).alias("prev_seq_no_avg_6"),
        F.ntile(5).over(w12).alias("next_prev_amount_bucket"),
    )
)

valid = (
    enriched_payments
    .filter(F.col("event_seq") != 4)
    .select(
        "id",
        "paid_at",
        "amount_share",
        "paid_at_to_date_avg_amount",
        "next_amount",
        "prev_amount",
        "next_seq_no",
        "prev_seq_no",
        "next_prev_amount",
        "event_seq",
        "event_seq_2",
        "last_known_prev_amount",
        "event_seq_3",
        "prev_seq_no_avg_6",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
    )
)

metrics = (
    valid
    .withColumn("cumulative_event_seq_2", F.sum("event_seq_2").over(w13))
    .withColumn("paid_at_to_date_avg_amount_bucket", F.ntile(3).over(w14))
    .withColumn("event_seq_3_bucket", F.ntile(3).over(w15))
)

metrics_payments = (
    metrics
    .select(
        "id",
        "paid_at",
        "amount_share",
        "next_amount",
        "prev_seq_no",
        "event_seq",
        "prev_seq_no_avg_6",
        "days_since_paid_at",
        "cumulative_event_seq_2",
        "paid_at_to_date_avg_amount_bucket",
        "event_seq_3_bucket",
        F.lag("paid_at_to_date_avg_amount").over(w14).alias("prev_paid_at_to_date_avg_amount"),
        F.lead("next_prev_amount").over(w14).alias("next_next_prev_amount"),
        F.row_number().over(w14).alias("seq_no"),
        F.sum("event_seq_3_bucket").over(w16).alias("running_event_seq_3_bucket"),
    )
)

result = (
    metrics_payments
    .withColumn("event_seq_3_bucket_tier", F.when(F.col("event_seq_3_bucket") >= 7, "HIGH").otherwise("LOW"))
    .withColumn("days_since_paid_at_tier", F.when(F.col("days_since_paid_at") >= 77, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["event_seq_3_bucket_tier", "days_since_paid_at_tier"], ["event_seq_3_bucket_tier"]],
        "event_seq_3_bucket_tier",
        "days_since_paid_at_tier",
    )
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.percentile_approx("days_since_paid_at", 0.95).alias("days_since_paid_at_p95"),
        F.avg("event_seq").alias("avg_event_seq"),
    )
    .orderBy(F.col("span_minutes").asc_nulls_first())
)
