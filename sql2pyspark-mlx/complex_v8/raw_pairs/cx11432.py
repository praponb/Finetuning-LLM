from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_closed_at_year = Window.partitionBy(F.year("closed_at"))
w_has_bug_starts_session_by_cl = Window.partitionBy("has_bug", "starts_session").orderBy(F.col("closed_at").desc(), "id").rowsBetween(-3, -1)
w_has_bug_starts_session_by_cl_2 = Window.partitionBy("has_bug", "starts_session").orderBy(F.col("closed_at").desc(), "id")

base = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        F.array_contains(F.col("tags"), "bug").alias("has_bug"),
    )
)

with_prev = base.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_channel_running))

windowed = (
    sessionized
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "has_bug",
        "prev_opened_at",
        "starts_session",
        "burst_id",
        F.count(F.lit(1)).over(w_closed_at_year).alias("rows_in_closed_at_year"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_closed_at_year), 2).alias("requester_id_vs_closed_at_year_avg"),
        F.round(F.avg("starts_session").over(w_has_bug_starts_session_by_cl), 2).alias("starts_session_avg_3_prior"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_has_bug_starts_session_by_cl_2))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.row_number().over(w_has_bug_starts_session_by_cl_2).alias("seq_no"),
        F.size(F.collect_set("channel").over(w_closed_at_year)).alias("distinct_channel_in_closed_at_year"),
    )
)

result = (
    windowed
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "has_bug",
        "starts_session",
        "burst_id",
        "rows_in_closed_at_year",
        "requester_id_vs_closed_at_year_avg",
        "seq_no",
        (F.col("closed_at") + F.expr("INTERVAL 15 MINUTES")).alias("closed_at_plus_15m"),
        (F.col("seq_no") - F.col("starts_session")).alias("seq_no_minus_starts_session"),
    )
    .orderBy(F.col("closed_at_plus_15m").desc(), F.col("burst_id").desc())
)
