from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")
w_event_at_month_by_event_at = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy("event_at", "id")
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_by_event_at_r = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc())
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id")
w_event_at_month_by_event_at_t = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy("event_at")
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id")
w_event_at_month_by_event_at_f = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_events = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(8, 12)) & (F.col("actor") != "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
)

with_parent = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.array_contains(F.col("t.tags"), "outage"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_prefix",
        F.col("t.tags").alias("ticket_tags"),
    )
)

ranked = (
    with_parent
    .select(
        "id",
        "event_at",
        "event_type_prefix",
        "ticket_tags",
        F.ntile(5).over(w_event_type_by_event_at).alias("event_at_quintile"),
        F.ntile(3).over(w_event_at_month_by_event_at).alias("event_at_tercile"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at"),
        F.first("event_at").over(w_ticket_id_by_event_at_full).alias("first_event_at"),
        F.last("event_at", ignorenulls=True).over(w_event_at_month_by_event_at_r).alias("last_known_event_at"),
        F.row_number().over(w_event_type_by_event_at).alias("rn"),
        F.last("event_at", ignorenulls=True).over(w_actor_ticket_id_by_event_at).alias("last_known_event_at_2"),
        F.row_number().over(w_event_at_month_by_event_at).alias("event_at_seq"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.lag("event_at", 3).over(w_event_at_month_by_event_at).alias("prev3_event_at"),
        F.lag("event_at").over(w_ticket_id_by_event_at).alias("prev_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_month_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.cume_dist().over(w_event_at_month_by_event_at_t), 3).alias("event_at_cume_dist_2"),
        F.ntile(10).over(w_actor_ticket_id_by_event_at_2).alias("event_at_decile"),
        F.first("event_at").over(w_event_at_month_by_event_at_f).alias("first_event_at_2"),
    )
)

result = (
    ranked
    .filter(F.col("first_event_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
    .select(
        "id",
        "event_at",
        "event_at_quintile",
        "event_at_tercile",
        "first_event_at",
        "last_known_event_at",
        "last_known_event_at_2",
        "event_at_seq",
        "event_at_cume_dist",
        "prev3_event_at",
        "minutes_since_prev_event_at",
        "event_at_cume_dist_2",
        "event_at_decile",
        "first_event_at_2",
        (F.col("event_at") + F.expr("INTERVAL 2 HOURS")).alias("event_at_plus_2h"),
        F.greatest(F.col("rn"), F.col("event_at_seq")).alias("greatest_rn_event_at_seq"),
    )
)
