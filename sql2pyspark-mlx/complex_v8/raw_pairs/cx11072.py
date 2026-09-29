from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_id_event_actor = Window.partitionBy("event_id", "event_actor")
w_event_actor_event_type_len_b = Window.partitionBy("event_actor", "event_type_len").orderBy(F.col("event_type_len").desc_nulls_first(), "id", "event_id")

valid = (
    df_ticket_events
    .filter((~F.col("event_type").isin("replied", "escalated")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

line_level_events = (
    valid.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type_len",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

metrics = (
    line_level_events
    .select(
        "*",
        F.size(F.collect_set("ticket_id").over(w_event_id_event_actor)).alias("distinct_ticket_id_in_event_id_event_actor"),
        F.row_number().over(w_event_actor_event_type_len_b).alias("event_type_len_seq"),
        F.lead("event_type_len", 2).over(w_event_actor_event_type_len_b).alias("next2_event_type_len"),
    )
)

with_activity = (
    metrics
    .crossJoin(metrics.agg(F.min("distinct_ticket_id_in_event_id_event_actor").alias("overall_min_distinct_ticket_id_in_event_id_event")))
    .filter(F.col("distinct_ticket_id_in_event_id_event_actor") > F.col("overall_min_distinct_ticket_id_in_event_id_event"))
    .drop("overall_min_distinct_ticket_id_in_event_id_event")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

profiled = (
    with_activity.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        "t.first_event_at",
        "t.first_event_at_2",
    )
)

combined_details_event = (
    profiled.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .select("s.*", "t.event_type", F.col("t.actor").alias("event_actor_2"))
)

result = (
    combined_details_event
    .groupBy("event_actor", "event_id")
    .pivot("event_type", ["replied", "escalated"])
    .agg(F.max("next2_event_type_len"))
)
