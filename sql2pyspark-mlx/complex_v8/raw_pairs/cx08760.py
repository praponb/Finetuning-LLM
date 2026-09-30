from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_group = Window.partitionBy("priority_group").orderBy("opened_at", "id")
w_priority_group_running = Window.partitionBy("priority_group").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_count = Window.orderBy("event_count", "priority_group", "session_no")
w_session_start_year_event_cou = Window.partitionBy("session_start_year", "event_count_tier").orderBy("event_count")
w_session_start_year = Window.partitionBy("session_start_year")

recent = (
    df_tickets
    .filter(F.col("agent_id") >= 7)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.when(F.col("priority").isin("P3", "P1"), "group_a").when(F.col("priority") == "P4", "group_b").otherwise("other").alias("priority_group"),
    )
)

with_prev = recent.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority_group))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_priority_group_running))

sessions = (
    sessionized
    .groupBy("priority_group", "session_no")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched = (
    sessions
    .filter(F.col("priority_group") == "group_b")
    .select(
        "priority_group",
        "session_no",
        "session_start",
        "session_end",
        "event_count",
        F.abs(F.col("duration_min") - 144.0).alias("duration_min_distance"),
        F.year(F.col("session_start")).alias("session_start_year"),
        F.when(F.col("event_count") >= 4, "large").when(F.col("event_count") >= 3, "medium").otherwise("small").alias("event_count_tier"),
    )
)

metrics = (
    enriched
    .select(
        "priority_group",
        "session_no",
        "session_start",
        "session_end",
        "event_count",
        "duration_min_distance",
        F.dense_rank().over(w_all_by_event_count).alias("event_count_dense_rank"),
        F.round(F.percent_rank().over(w_session_start_year_event_cou), 3).alias("event_count_percent_rank"),
        F.size(F.collect_set("priority_group").over(w_session_start_year)).alias("distinct_priority_group_in_session_start_year"),
    )
)

result = (
    metrics
    .select(
        "priority_group",
        "session_no",
        "distinct_priority_group_in_session_start_year",
        "event_count_dense_rank",
        "event_count",
    )
    .unpivot(
        ids=["priority_group", "session_no"],
        values=["distinct_priority_group_in_session_start_year", "event_count_dense_rank", "event_count"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
