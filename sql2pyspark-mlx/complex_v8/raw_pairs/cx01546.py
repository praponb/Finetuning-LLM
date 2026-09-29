from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy("event_at", "id")
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").desc())
w_all_by_event_at = Window.orderBy("event_at", "id")
w_actor_ticket_id_by_event_at_3 = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id")

prepared = df_ticket_events.filter(F.col("event_type") != "assigned")

with_parent = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        F.col("t.priority").alias("ticket_priority"),
    )
)

ranked = (
    with_parent
    .select(
        "*",
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.ntile(10).over(w_ticket_id_actor_by_event_at).alias("event_at_decile"),
        F.rank().over(w_actor_ticket_id_by_event_at_2).alias("event_at_rank"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
        F.lead("event_at", 2).over(w_all_by_event_at).alias("next2_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_ticket_id_by_event_at_3))) / 60, 1).alias("minutes_since_prev_event_at_3"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ticket_id",
        "actor",
        "ticket_priority",
        "minutes_since_prev_event_at",
        "event_at_decile",
        "event_at_rank",
        "minutes_since_prev_event_at_2",
        "prev_event_at",
        "event_at_cume_dist",
        "next2_event_at",
        F.round(F.col("event_at_cume_dist") * 1.07, 2).alias("event_at_cume_dist_adj"),
    )
    .orderBy("event_at_cume_dist", "ticket_id")
)
