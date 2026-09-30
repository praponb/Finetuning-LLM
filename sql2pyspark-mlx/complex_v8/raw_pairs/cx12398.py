from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at_hour").desc(), "id")
w_all_by_event_at_hour_moving = Window.orderBy(F.col("event_at_hour").desc(), "id").rowsBetween(-3, Window.currentRow)
w_event_at_hour_ticket_id_by_e = Window.partitionBy("event_at_hour", "ticket_id").orderBy("event_at_hour", "id")
w_actor = Window.partitionBy("actor")
w_event_at_hour = Window.partitionBy("event_at_hour")
w_all_by_event_at_hour_ties = Window.orderBy("event_at_hour")
w_event_at_hour_ticket_id_by_e_2 = Window.partitionBy("event_at_hour", "ticket_id").orderBy("event_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_hour_ties_2 = Window.orderBy(F.col("event_at_hour").desc())

scoped_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
        F.hour(F.col("event_at")).alias("event_at_hour"),
    )
)

with_details_ticket = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.size(F.col("t.tags")) >= 2)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_type_prefix",
        "s.event_at_hour",
        F.col("t.tags").alias("ticket_tags"),
    )
)

windowed = (
    with_details_ticket
    .select(
        "id",
        "actor",
        "event_at_hour",
        "ticket_tags",
        F.round(F.cume_dist().over(w_event_type_actor_by_event_at), 3).alias("event_at_hour_cume_dist"),
        F.round(F.avg("event_at_hour").over(w_all_by_event_at_hour_moving), 2).alias("event_at_hour_avg_4"),
        (F.col("event_at_hour") - F.lag("event_at_hour").over(w_event_at_hour_ticket_id_by_e)).alias("event_at_hour_change"),
        F.round(F.col("event_at_hour") / F.nullif(F.sum("event_at_hour").over(w_actor), F.lit(0)), 4).alias("event_at_hour_share_of_actor"),
        F.max("event_at_hour").over(w_event_at_hour).alias("event_at_hour_max_event_at_hour"),
        F.round(F.percent_rank().over(w_all_by_event_at_hour_ties), 3).alias("event_at_hour_percent_rank"),
        F.size(F.collect_set("event_type").over(w_actor)).alias("distinct_event_type_in_actor"),
        F.first("event_at_hour").over(w_event_at_hour_ticket_id_by_e_2).alias("first_event_at_hour"),
        F.rank().over(w_all_by_event_at_hour_ties_2).alias("event_at_hour_rank"),
        F.round(F.avg("event_at_hour").over(w_actor), 2).alias("actor_avg_event_at_hour"),
        F.row_number().over(w_event_at_hour_ticket_id_by_e).alias("seq_no"),
    )
)

leaders = windowed.filter(F.col("event_at_hour_rank") <= 5)

result = (
    leaders
    .filter(F.col("event_at_hour_share_of_actor") <= 0.7)
    .select(
        "id",
        "actor",
        "event_at_hour",
        "ticket_tags",
        "event_at_hour_avg_4",
        "event_at_hour_change",
        "event_at_hour_share_of_actor",
        "event_at_hour_max_event_at_hour",
        "event_at_hour_percent_rank",
        "distinct_event_type_in_actor",
        "event_at_hour_rank",
        "actor_avg_event_at_hour",
        "seq_no",
        F.round(F.col("actor_avg_event_at_hour") * 0.25, 2).alias("actor_avg_event_at_hour_adj"),
        F.upper(F.trim(F.col("actor"))).alias("actor_clean"),
    )
    .orderBy("seq_no")
)
