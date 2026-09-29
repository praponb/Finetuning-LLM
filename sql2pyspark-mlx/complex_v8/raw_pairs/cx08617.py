from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy("event_at", "id")
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").desc(), "id")
w_event_type_actor_by_event_at_3 = Window.partitionBy("event_type", "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy("event_at")
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").desc())

filtered_events = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id") <= 18), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .filter(F.col("t.channel") == "email")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.tags").alias("ticket_tags"),
        "su.agent_name",
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.nth_value("event_at", 2).over(w_event_type_actor_by_event_at).alias("second_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_actor_by_event_at_2))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lead("event_at").over(w_actor_by_event_at).alias("next_event_at"),
        F.ntile(3).over(w_event_type_actor_by_event_at_2).alias("event_at_tercile"),
        F.lag("event_at").over(w_actor_ticket_id_by_event_at).alias("prev_event_at"),
        F.lead("event_at").over(w_event_type_actor_by_event_at_2).alias("next_event_at_2"),
        F.first("event_at").over(w_event_type_actor_by_event_at).alias("first_event_at"),
        F.row_number().over(w_actor_ticket_id_by_event_at).alias("seq_no"),
        F.ntile(5).over(w_actor_ticket_id_by_event_at).alias("event_at_quintile"),
        F.first("event_at", ignorenulls=True).over(w_event_type_actor_by_event_at_3).alias("first_known_event_at"),
        F.round(F.cume_dist().over(w_event_type_actor_by_event_at_2), 3).alias("event_at_cume_dist"),
        F.rank().over(w_actor_by_event_at_ties).alias("event_at_rank"),
        F.round(F.percent_rank().over(w_actor_ticket_id_by_event_at_2), 3).alias("event_at_percent_rank"),
    )
)

labeled = (
    ranked
    .filter(F.col("ticket_id") <= 29)
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("first_event_at")).alias("days_since_first_event_at"),
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

result = (
    labeled
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "ticket_channel",
        "ticket_tags",
        "agent_name",
        "second_event_at",
        "minutes_since_prev_event_at",
        "next_event_at",
        "event_at_tercile",
        "next_event_at_2",
        "seq_no",
        "event_at_quintile",
        "first_known_event_at",
        "event_at_cume_dist",
        "event_at_rank",
        "event_at_percent_rank",
        "days_since_first_event_at",
        "event_type_clean",
        F.size(F.col("ticket_tags")).alias("ticket_tags_count"),
        (F.col("event_at_percent_rank") - F.col("seq_no")).alias("event_at_percent_rank_minus_seq_no"),
    )
    .orderBy(F.col("ticket_id").desc(), F.col("event_at_cume_dist").desc(), "id")
    .limit(10)
)
