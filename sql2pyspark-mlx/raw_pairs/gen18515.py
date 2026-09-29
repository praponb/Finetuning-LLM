from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("WEEK", "prev_paid_at"), "status").orderBy("prev_paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.date_trunc("MONTH", "prev_paid_at"), "method")
w5 = Window.partitionBy("status", "method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("adjusted_amount", "id")
w7 = Window.partitionBy("status").orderBy("prev_paid_at_period_count_session_start_flag", "id").rowsBetween(-3, -1)
w8 = Window.partitionBy(F.date_trunc("YEAR", "paid_at"), "status").orderBy(F.col("prev_paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w9 = Window.partitionBy(F.to_date("prev_paid_at"), "status").orderBy("first_known_session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("status").orderBy(F.col("prev_paid_at_period_count_session_start_flag").desc(), "id")

valid = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("paid_at") >= F.lit("2023-08-27 15:00:00").cast("timestamp")))
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.round((F.col("amount") * 100), 2).alias("adjusted_amount"),
    )
)

base = (
    valid
    .filter((F.col("adjusted_amount") > 43) & (~F.col("method").eqNullSafe(F.lit("cash"))))
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "amount_filled",
        "adjusted_amount",
        F.substring("status", 1, 3).alias("status_prefix"),
    )
)

prev_events = base.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

with_windows = (
    sessionized
    .select(
        "id",
        "paid_at",
        "status",
        "amount_filled",
        "adjusted_amount",
        "status_prefix",
        "prev_paid_at",
        F.first("amount_filled", ignorenulls=True).over(w3).alias("first_known_amount_filled"),
        F.count("session_start_flag").over(w4).alias("prev_paid_at_period_count_session_start_flag"),
        F.first("session_start_flag", ignorenulls=True).over(w5).alias("first_known_session_start_flag"),
    )
)

enriched = (
    with_windows
    .filter(F.col("status") == "SUCCESS")
    .withColumn("next_amount_filled", F.lead("amount_filled", 2).over(w6))
    .withColumn("adjusted_amount_avg_3_prior", F.avg("adjusted_amount").over(w7))
    .withColumn("amount_filled_max_7d", F.max("amount_filled").over(w8))
    .withColumn("running_amount_filled", F.sum("amount_filled").over(w9))
    .withColumn("seq_no", F.row_number().over(w10))
)

result = (
    enriched
    .withColumn("quarter_start", F.date_trunc("QUARTER", "paid_at"))
    .withColumn("first_known_amount_filled_tier", F.when(F.col("first_known_amount_filled") >= 62, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["seq_no", "quarter_start", "first_known_amount_filled_tier"], ["seq_no", "quarter_start"], ["seq_no"], []],
        "seq_no",
        "quarter_start",
        "first_known_amount_filled_tier",
    )
    .agg(
        F.round(F.sum("first_known_session_start_flag") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("first_known_session_start_flag_per_success"),
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
    )
    .orderBy(F.col("seq_no").desc_nulls_last())
)
