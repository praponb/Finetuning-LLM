from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

with_prev = valid_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_event_type_running))

session_summary = (
    sessionized
    .groupBy("event_type", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

stats = (
    session_summary
    .filter(F.col("events") > 2)
    .groupBy(
        F.date_trunc("month", "session_start").alias("session_start_month"),
        F.year("session_end").alias("session_end_year"),
    )
    .agg(
        F.sum("events").alias("total_events"),
        F.count("*").alias("cnt"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.max("events").alias("max_events"),
        F.sum("burst_id").alias("total_burst_id"),
    )
)

qualified = (
    stats
    .crossJoin(stats.agg(F.avg("distinct_event_types").alias("overall_avg_distinct_event_types")))
    .filter(F.col("distinct_event_types") > F.col("overall_avg_distinct_event_types"))
    .drop("overall_avg_distinct_event_types")
)

result = (
    qualified
    .select(
        "session_start_month",
        "session_end_year",
        "distinct_event_types",
        "cnt",
        "total_burst_id",
    )
    .unpivot(
        ids=["session_start_month", "session_end_year"],
        values=["distinct_event_types", "cnt", "total_burst_id"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
