from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_id = Window.partitionBy("actor", "ticket_id")
w_event_at_month_by_event_at_f = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_by_event_at = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at").desc(), "id")

base_events = (
    df_ticket_events
    .filter(~F.col("event_type").isin("resolved", "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label_2"),
    )
)

windowed = (
    base_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_label",
        "days_since_event_at",
        "event_at_label_2",
        F.size(F.collect_set("event_type").over(w_actor_ticket_id)).alias("distinct_event_type_in_actor_ticket_id"),
        F.last("event_at").over(w_event_at_month_by_event_at_f).alias("last_event_at"),
        F.row_number().over(w_event_at_month_by_event_at).alias("seq_no"),
    )
)

screened = (
    windowed.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isNotNull()), "left_anti")
    .filter(F.col("ticket_id").isNotNull())
)

result = (
    screened
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "days_since_event_at",
        "event_at_label_2",
        "distinct_event_type_in_actor_ticket_id",
        "last_event_at",
        "seq_no",
        F.concat_ws(" / ", F.col("event_at_label_2"), F.col("event_type")).alias("event_at_label_2_event_type_label"),
    )
    .orderBy(F.col("event_at_label_2").desc())
)
