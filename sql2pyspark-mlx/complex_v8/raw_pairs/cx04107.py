from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_to_date = Window.partitionBy(F.to_date("closed_at"))
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")
w_all_by_opened_at_ties = Window.orderBy("opened_at")
w_all_by_closed_at_ties = Window.orderBy(F.col("closed_at").desc())

scoped_tickets = (
    df_tickets
    .filter((F.col("opened_at") >= F.lit("2025-01-13 07:00:00").cast("timestamp")) & (F.col("agent_id").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.date_trunc("hour", F.col("closed_at")).alias("closed_at_hour"),
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
    )
)

joined_agent = (
    scoped_tickets.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.agent_name").isNotNull()), "inner")
    .select("s.*", "su.agent_name")
)

sequenced = (
    joined_agent
    .select(
        "*",
        F.min("requester_id_filled").over(w_closed_at_to_date).alias("closed_at_to_date_min_requester_id_filled"),
        F.size(F.collect_set("agent_id").over(w_closed_at_to_date)).alias("distinct_agent_id_in_closed_at_to_date"),
        F.rank().over(w_all_by_closed_at).alias("closed_at_rank"),
        F.round(F.cume_dist().over(w_all_by_opened_at_ties), 3).alias("opened_at_cume_dist"),
        F.rank().over(w_all_by_closed_at_ties).alias("closed_at_rank_2"),
    )
)

exploded = (
    sequenced
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "closed_at_hour",
        "requester_id_filled",
        "agent_name",
        "closed_at_to_date_min_requester_id_filled",
        "distinct_agent_id_in_closed_at_to_date",
        "closed_at_rank",
        "opened_at_cume_dist",
        "closed_at_rank_2",
        F.posexplode_outer("tags").alias("tag_pos", "tag"),
    )
)

exploded_detail_events = (
    exploded.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.to_date(F.col("t.event_at")) < F.lit("2025-01-31").cast("date")), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.closed_at_hour",
        "s.requester_id_filled",
        "s.distinct_agent_id_in_closed_at_to_date",
        "s.closed_at_rank",
        "s.opened_at_cume_dist",
        "s.closed_at_rank_2",
        "s.tag_pos",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    exploded_detail_events
    .groupingSets(
        [["csat", "distinct_agent_id_in_closed_at_to_date"], ["csat"], ["distinct_agent_id_in_closed_at_to_date"], []],
        "csat", "distinct_agent_id_in_closed_at_to_date",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(~F.col("priority").isin("P1", "P3"), F.col("closed_at_rank_2")).otherwise(0)).alias("closed_at_rank_2_priority_not_in_p1_p3"),
        F.count("*").alias("cnt"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
    .orderBy(
        F.col("csat").asc_nulls_last(),
        F.col("distinct_agent_id_in_closed_at_to_date").asc_nulls_last(),
    )
)
