from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_total_duration_min_mo = Window.orderBy("total_duration_min", "session_end_month", "events").rowsBetween(-2, Window.currentRow)
w_all_by_total_duration_min = Window.orderBy("total_duration_min", "session_end_month", "events")
w_n_channel_is_not_null_events = Window.partitionBy("n_channel_is_not_null", "events")
w_all_by_total_duration_min_pr = Window.orderBy("total_duration_min", "session_end_month", "events").rowsBetween(-6, -1)

filtered = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "bug"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
    )
)

with_prev = filtered.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_channel_running))

per_session = (
    sessionized
    .groupBy("channel", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

totals = (
    per_session
    .groupBy(
        F.date_trunc("month", "session_end").alias("session_end_month"),
        "events",
    )
    .agg(
        F.round(F.sum("duration_min"), 2).alias("total_duration_min"),
        F.count(F.when(F.col("channel").isNotNull(), 1)).alias("n_channel_is_not_null"),
    )
)

with_trends = (
    totals
    .select(
        "*",
        F.max("events").over(w_all_by_total_duration_min_mo).alias("events_max_3"),
        F.ntile(3).over(w_all_by_total_duration_min).alias("total_duration_min_tercile"),
        F.sum("total_duration_min").over(w_n_channel_is_not_null_events).alias("n_channel_is_not_null_events_sum_total_duration_"),
        F.sum("total_duration_min").over(w_all_by_total_duration_min_pr).alias("total_duration_min_sum_6_prior"),
    )
)

result = (
    with_trends
    .select(
        "session_end_month",
        "events",
        "total_duration_min",
        "n_channel_is_not_null",
        "events_max_3",
        "total_duration_min_tercile",
        "n_channel_is_not_null_events_sum_total_duration_",
        "total_duration_min_sum_6_prior",
        F.when(F.col("total_duration_min") >= 864.0, "large").when(F.col("total_duration_min") >= 360.0, "medium").otherwise("small").alias("total_duration_min_tier"),
    )
    .orderBy(F.col("session_end_month").desc_nulls_first(), "events")
    .limit(10)
)
