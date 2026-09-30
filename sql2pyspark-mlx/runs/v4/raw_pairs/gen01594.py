from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-11-19").cast("date")) & F.col("order_id").isNotNull() & (F.col("amount") >= 550))
    .select(
        "*",
        F.concat_ws("-", "method", "status").alias("method_status_key"),
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
    )
)

prev_events = base.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_payments
    .groupBy(
        F.when(F.col("amount") >= 1330, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.min("session_start_flag").alias("min_session_start_flag"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("prev_paid_at")) - F.unix_timestamp(F.min("prev_paid_at"))) / 60).alias("duration_min_2"),
    )
    .filter(F.col("min_session_start_flag") >= 0)
)
