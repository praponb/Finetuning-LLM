from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "method",
        "paid_at",
        F.when(F.col("status") == "REFUNDED", 1).otherwise(0).alias("is_refunded"),
    )
)

prev_events = cleaned.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .withColumn("is_refunded_tier", F.when(F.col("is_refunded") >= 82, "PREMIUM").otherwise("STANDARD"))
    .withColumn("session_start_flag_tier", F.when(F.col("session_start_flag") >= 1, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["activity_month", "is_refunded_tier", "session_start_flag_tier"], ["activity_month", "is_refunded_tier"], []],
        "activity_month",
        "is_refunded_tier",
        "session_start_flag_tier",
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.max("prev_paid_at").alias("last_seen"),
        F.percentile_approx("is_refunded", 0.95).alias("is_refunded_p95"),
    )
)
