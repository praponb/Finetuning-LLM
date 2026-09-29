from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id")
w_actor_event_at_to_date_by_ev = Window.partitionBy("actor", F.to_date("event_at")).orderBy("event_at_week", "id")
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_2 = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_by_event_at_runnin = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_event_at_to_date_by_ev_2 = Window.partitionBy("actor", F.to_date("event_at")).orderBy("event_at_week", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_ticket_events
    .filter((F.col("event_type").isNotNull()) & (F.hour(F.col("event_at")).between(10, 16)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

ranked = (
    scoped
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_at_week",
        F.row_number().over(w_ticket_id_by_event_at).alias("seq_no"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_event_at_to_date_by_ev))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at"),
        F.lead("event_at").over(w_actor_event_at_to_date_by_ev).alias("next_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at_3"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at_2), 3).alias("event_at_cume_dist"),
        F.round((F.unix_timestamp("event_at_week") - F.unix_timestamp(F.lag("event_at_week").over(w_ticket_id_by_event_at_2))) / 60, 1).alias("minutes_since_prev_event_at_week"),
        F.row_number().over(w_ticket_id_by_event_at_2).alias("seq_no_2"),
        F.first("event_at_week", ignorenulls=True).over(w_ticket_id_by_event_at_runnin).alias("first_known_event_at_week"),
        F.first("event_at_week").over(w_actor_event_at_to_date_by_ev_2).alias("first_event_at_week"),
        F.rank().over(w_ticket_id_by_event_at).alias("event_at_rank"),
    )
)

leaders = ranked.filter((F.col("event_at_rank") <= 2) & (F.month(F.col("event_at")).isin(1, 3)))

result = (
    leaders
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_at_week",
        "seq_no",
        "minutes_since_prev_event_at",
        "event_at_cume_dist",
        "minutes_since_prev_event_at_week",
        "first_event_at_week",
        "event_at_rank",
        F.concat(F.col("event_type"), F.lit("#"), F.col("event_at_rank").cast("string")).alias("event_type_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
    .orderBy("id", "event_at_rank")
)
