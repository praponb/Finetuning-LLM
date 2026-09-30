from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-09-23").cast("date")) & (F.col("method").eqNullSafe(F.lit("promptpay"))))
    .withColumn("status_prefix", F.substring("status", 1, 2))
)

flagged = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("island_id", F.sum("session_start_flag").over(w2))

with_gaps = (
    sessions_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w3).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w3) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments_2 = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w4))

cleaned = (
    sessions_payments_2
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-08-04"))) & (F.col("session_start_flag") < 0))
    .withColumn("session_id_minus_is_new_burst", (F.col("session_id") - F.col("is_new_burst")))
    .withColumn("is_new_burst_times_amount", (F.col("is_new_burst") * F.col("amount")))
)

gaps = (
    cleaned
    .withColumn("session_start_flag_2", F.when(F.lag("paid_at").over(w3).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w3) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments_3 = (
    gaps
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        "session_start_flag",
        "island_id",
        "is_new_burst",
        "session_id",
        "session_id_minus_is_new_burst",
        "is_new_burst_times_amount",
        F.sum("session_start_flag_2").over(w4).alias("island_id_2"),
    )
)

stats = (
    sessions_payments_3
    .groupBy(
        F.when(F.col("session_id_minus_is_new_burst") >= 85, "PREMIUM").otherwise("STANDARD").alias("session_id_minus_is_new_burst_tier"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("session_start_flag", [0.25, 0.5, 0.75]).alias("session_start_flag_quartiles"),
    )
)

result = (
    stats
    .select(
        "session_id_minus_is_new_burst_tier",
        "success_amount",
        "success_count",
        "distinct_ids",
        "session_start_flag_quartiles",
    )
    .orderBy("distinct_ids")
)
