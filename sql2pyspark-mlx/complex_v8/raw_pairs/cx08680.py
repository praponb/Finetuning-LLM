from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_csat_event_type_by_ti = Window.partitionBy("ticket_csat", "event_type").orderBy("ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_csat_event_type_by_ti_2 = Window.partitionBy("ticket_csat", "event_type").orderBy("ticket_csat")
w_actor = Window.partitionBy("actor")
w_ticket_csat_event_type_by_ti_3 = Window.partitionBy("ticket_csat", "event_type").orderBy("ticket_csat", "id").rowsBetween(-1, -1)
w_ticket_priority_by_ticket_cs = Window.partitionBy("ticket_priority").orderBy("ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(9, 15)) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

enriched = (
    valid.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.actor_event_type_label",
        F.col("t.channel").alias("ticket_channel"),
    )
)

joined_ticket = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.actor_event_type_label",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

sequenced = (
    joined_ticket
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_event_type_label",
        "ticket_priority",
        "ticket_csat",
        F.nth_value("ticket_csat", 2).over(w_ticket_csat_event_type_by_ti).alias("second_ticket_csat"),
        F.dense_rank().over(w_ticket_csat_event_type_by_ti_2).alias("ticket_csat_dense_rank"),
        F.size(F.collect_set("ticket_id").over(w_actor)).alias("distinct_ticket_id_in_actor"),
        F.max("ticket_csat").over(w_ticket_csat_event_type_by_ti_3).alias("ticket_csat_max_1_prior"),
        F.first("ticket_csat").over(w_ticket_priority_by_ticket_cs).alias("first_ticket_csat"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_event_type_label",
        "ticket_priority",
        "ticket_csat",
        "second_ticket_csat",
        "distinct_ticket_id_in_actor",
        "ticket_csat_max_1_prior",
        "first_ticket_csat",
        F.concat_ws("|", F.col("actor_event_type_label"), F.col("actor")).alias("actor_event_type_label_actor_label"),
    )
    .orderBy(F.col("ticket_csat").desc(), F.col("event_type").desc_nulls_first(), "id")
    .limit(20)
)
