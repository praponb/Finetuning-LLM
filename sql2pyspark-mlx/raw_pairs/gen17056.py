from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("paid_at", "id")
w4 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("method").orderBy("amount", "id")
w6 = Window.partitionBy(F.year("paid_at")).orderBy("amount_avg_5", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.partitionBy("method").orderBy(F.col("prev_session_id").desc(), "id")
w8 = Window.partitionBy("method").orderBy("prev_session_id", "id")
w9 = Window.partitionBy("method").orderBy("prev_session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("method").orderBy("amount_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("method").orderBy("row_seq", "id")

with_windows = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        F.sum("amount").over(w).alias("cumulative_amount"),
        F.avg("amount").over(w2).alias("amount_avg_5"),
    )
)

with_gaps = (
    with_windows
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w3) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "amount_avg_5",
        F.sum("new_group_flag").over(w4).alias("session_id"),
    )
)

enriched = (
    sessionized
    .select(
        "id",
        "method",
        "amount_avg_5",
        F.lag("session_id").over(w_part).alias("prev_session_id"),
        F.lag("amount_avg_5").over(w_part).alias("prev_amount_avg_5"),
        F.lead("amount_avg_5").over(w_part).alias("next_amount_avg_5"),
        F.row_number().over(w_part).alias("row_seq"),
        F.avg("amount").over(w6).alias("amount_avg_2"),
    )
)

filtered = (
    enriched
    .filter(F.col("method").isNotNull() & F.col("prev_session_id").isNotNull())
    .select("*", F.col("amount_avg_2").cast("string").alias("amount_avg_2_num"))
)

scored = (
    filtered
    .select(
        "id",
        "method",
        "amount_avg_5",
        "prev_session_id",
        "prev_amount_avg_5",
        "next_amount_avg_5",
        "row_seq",
        "amount_avg_2",
        "amount_avg_2_num",
        F.ntile(3).over(w7).alias("prev_session_id_bucket"),
        F.ntile(10).over(w8).alias("prev_session_id_bucket_2"),
        F.sum("prev_amount_avg_5").over(w9).alias("running_prev_amount_avg_5"),
        F.last("prev_session_id", ignorenulls=True).over(w10).alias("last_known_prev_session_id"),
        F.row_number().over(w11).alias("event_seq"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("last_known_prev_session_id") >= 8, "HIGH").otherwise("LOW").alias("last_known_prev_session_id_tier"),
    )
    .agg(
        F.percentile_approx("next_amount_avg_5", 0.5).alias("next_amount_avg_5_p50"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_rows"),
        F.size(F.collect_set("method")).alias("num_methods"),
    )
    .orderBy(F.col("bank_transfer_rows").desc())
)
