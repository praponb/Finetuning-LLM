from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("event_type", "actor").orderBy(F.col("session_id").desc(), F.col("id").desc())
w_event_type_by_session_id_run = Window.partitionBy("event_type").orderBy("session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type = Window.partitionBy("event_type")
w_event_type_by_session_id = Window.partitionBy("event_type").orderBy("session_id", "id")

filtered = (
    df_ticket_events
    .filter(F.col("actor") != "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
    )
)

with_prev = filtered.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_actor_running))

latest = (
    sessionized
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

ranked = (
    latest
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_label",
        "prev_event_at",
        "starts_session",
        "session_id",
        F.sum("starts_session").over(w_event_type_by_session_id_run).alias("running_starts_session"),
        F.round(F.col("session_id") - F.avg("session_id").over(w_event_type), 2).alias("session_id_vs_event_type_avg"),
        F.ntile(10).over(w_event_type_by_session_id).alias("session_id_decile"),
    )
)

result = (
    ranked
    .select(
        "id",
        "running_starts_session",
        "session_id_decile",
        "starts_session",
        "session_id",
    )
    .unpivot(
        ids=["id"],
        values=["running_starts_session", "session_id_decile", "starts_session", "session_id"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "measure")
)
