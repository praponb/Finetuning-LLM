from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("closed_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_end_year_by_session = Window.partitionBy(F.year("session_end")).orderBy("session_start", "priority", "session_no").rowsBetween(-3, Window.currentRow)
w_session_start_to_date = Window.partitionBy(F.to_date("session_start"))

recent = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
    )
)

with_prev = recent.withColumn("prev_closed_at", F.lag("closed_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at").isNull() | ((F.unix_timestamp("closed_at") - F.unix_timestamp("prev_closed_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_priority_running))

per_session = (
    sessionized
    .groupBy("priority", "session_no")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

scored = (
    per_session
    .filter(~F.col("priority").isin("P4", "P2"))
    .select(
        "*",
        F.when(F.col("priority").isin("P1", "P4"), "group_a").when(F.col("priority") == "P2", "group_b").otherwise("other").alias("priority_group"),
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.concat(F.col("priority"), F.lit("#"), F.col("event_count").cast("string")).alias("priority_ref"),
    )
)

windowed = (
    scored
    .select(
        "priority",
        "session_no",
        "session_end",
        "event_count",
        "priority_group",
        "priority_slug",
        "priority_ref",
        F.round(F.avg("event_count").over(w_session_end_year_by_session), 2).alias("event_count_avg_4"),
        F.min("event_count").over(w_session_start_to_date).alias("session_start_to_date_min_event_count"),
    )
)

result = (
    windowed
    .groupBy("session_end", "session_no")
    .pivot("priority_group", ["group_b", "other", "group_a"])
    .agg(F.max("event_count"))
    .orderBy("session_end", "session_no")
)
