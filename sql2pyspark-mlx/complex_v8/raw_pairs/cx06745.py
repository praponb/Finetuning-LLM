from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_start_year_event_typ = Window.partitionBy(F.year("session_start"), "event_type_group")
w_session_end_month_event_type = Window.partitionBy(F.date_trunc("month", "session_end"), "event_type").orderBy("session_start", "event_type", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = valid_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_event_type_running))

per_session = (
    sessionized
    .groupBy("event_type", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

derived = (
    per_session
    .filter(F.col("session_id") < 4)
    .select(
        "*",
        F.date_format(F.col("session_end"), "yyyy-MM").alias("session_end_label"),
        F.when(F.col("event_type").isin("assigned", "escalated"), "group_a").when(F.col("event_type") == "created", "group_b").otherwise("other").alias("event_type_group"),
        F.round(F.col("duration_min") * 1.07, 2).alias("duration_min_adj"),
    )
)

windowed = (
    derived
    .select(
        "event_type",
        "session_id",
        "session_end",
        "duration_min",
        "session_end_label",
        "duration_min_adj",
        F.max("event_count").over(w_session_start_year_event_typ).alias("session_start_year_event_type_group_max_event_co"),
        F.max("duration_min").over(w_session_start_year_event_typ).alias("session_start_year_event_type_group_max_duration"),
        F.sum("duration_min").over(w_session_end_month_event_type).alias("running_duration_min"),
        F.count(F.lit(1)).over(w_session_start_year_event_typ).alias("rows_in_session_start_year_event_type_group"),
    )
)

result = (
    windowed
    .groupBy("session_id", "session_end")
    .pivot("event_type", ["created", "reopened", "replied"])
    .agg(F.count("rows_in_session_start_year_event_type_group"))
    .orderBy("session_id", "session_end")
)
