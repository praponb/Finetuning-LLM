from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_ticket_id = Window.partitionBy(F.month("event_at"), "ticket_id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")

prepared_events = (
    df_ticket_events
    .filter((F.col("event_type") == "assigned") | (F.col("ticket_id") <= 48))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_activity = (
    prepared_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isin("system", "agent")) & (F.col("t.event_at") < F.lit("2025-02-12 13:00:00").cast("timestamp")), "left_semi")
)

windowed = (
    with_activity
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "actor_slug",
        "days_since_event_at",
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_event_at_month_ticket_id), F.lit(0)), 4).alias("days_since_event_at_share_of_event_at_month_tick"),
        F.ntile(10).over(w_all_by_event_at).alias("event_at_decile"),
        F.row_number().over(w_all_by_event_at).alias("rn"),
        F.min("days_since_event_at").over(w_event_at_month_ticket_id).alias("event_at_month_ticket_id_min_days_since_event_at"),
    )
)

result = (
    windowed
    .select(
        "id",
        "rn",
        "event_at_month_ticket_id_min_days_since_event_at",
        "days_since_event_at",
    )
    .unpivot(
        ids=["id"],
        values=["rn", "event_at_month_ticket_id_min_days_since_event_at", "days_since_event_at"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "measure")
)
