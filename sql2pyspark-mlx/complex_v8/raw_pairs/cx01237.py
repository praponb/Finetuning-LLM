from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_type = Window.partitionBy("actor", "event_type")
w_all_by_actor_len = Window.orderBy("actor_len", "id")
w_all_by_actor_len_ties = Window.orderBy("actor_len")

cleaned_events = (
    df_ticket_events
    .filter((F.col("actor") == "system") & (F.col("event_at") < F.lit("2025-02-24 08:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
)

with_trends = (
    cleaned_events
    .select(
        "*",
        F.min("actor_len").over(w_actor_event_type).alias("actor_event_type_min_actor_len"),
        F.lead("actor_len", 1, 0).over(w_all_by_actor_len).alias("next_actor_len"),
        F.rank().over(w_all_by_actor_len_ties).alias("actor_len_rank"),
        F.round(F.percent_rank().over(w_all_by_actor_len_ties), 3).alias("actor_len_percent_rank"),
        F.round(F.col("actor_len") - F.avg("actor_len").over(w_actor_event_type), 2).alias("actor_len_vs_actor_event_type_avg"),
    )
)

joined = (
    with_trends.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.actor_len",
        "s.next_actor_len",
        "s.actor_len_rank",
        "s.actor_len_percent_rank",
        "s.actor_len_vs_actor_event_type_avg",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

result = (
    joined
    .filter(F.size(F.col("ticket_tags")) >= 2)
    .select(
        "id",
        "ticket_id",
        "actor",
        "actor_len",
        "next_actor_len",
        "actor_len_rank",
        "actor_len_vs_actor_event_type_avg",
        "ticket_tags",
        "ticket_priority",
        F.length(F.trim(F.col("ticket_priority"))).alias("ticket_priority_len"),
        F.abs(F.col("actor_len") - 10).alias("actor_len_distance"),
    )
    .orderBy(F.col("actor_len").desc())
)
