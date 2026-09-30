from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_plus_2h = Window.orderBy("event_at_plus_2h", "id")
w_actor_actor_or_unknown_by_ev = Window.partitionBy("actor", "actor_or_unknown").orderBy("event_at_plus_2h", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_actor_or_unknown_by_ev_2 = Window.partitionBy("actor", "actor_or_unknown").orderBy("event_at_plus_2h", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id")

prepared_events = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 22) & (F.col("actor") != "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_activity = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.channel").isin("phone", "email", "web")), "left_semi")
)

derived = (
    with_activity
    .filter(F.col("ticket_id") <= 4)
    .select(
        "*",
        (F.col("event_at") + F.expr("INTERVAL 2 HOURS")).alias("event_at_plus_2h"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

screened = (
    derived.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.channel").isin("web", "chat")), "left_anti")
)

screened_2 = (
    screened.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

sequenced = (
    screened_2
    .select(
        "id",
        "event_at",
        "actor",
        "event_at_plus_2h",
        F.lead("event_at_plus_2h", 3).over(w_all_by_event_at_plus_2h).alias("next3_event_at_plus_2h"),
        F.last("event_at").over(w_actor_actor_or_unknown_by_ev).alias("last_event_at"),
        F.last("event_at", ignorenulls=True).over(w_actor_actor_or_unknown_by_ev_2).alias("last_known_event_at"),
        F.row_number().over(w_actor_by_event_at).alias("rn"),
        F.lag("event_at").over(w_actor_by_event_at).alias("prev_event_at"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "event_at",
        "event_at_plus_2h",
        "last_event_at",
        "rn",
        "prev_event_at",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.round((F.unix_timestamp(F.col("prev_event_at")) - F.unix_timestamp(F.col("last_event_at"))) / 60, 1).alias("minutes_last_event_at_to_prev_event_at"),
    )
    .orderBy(F.col("event_at").desc_nulls_first(), F.col("actor_slug").desc(), "id")
    .limit(10)
)
