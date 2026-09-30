from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_ticket_requeste = Window.partitionBy("ticket_id").orderBy(F.col("ticket_requester_id").desc(), "id").rowsBetween(-1, Window.currentRow)
w_agent_hired_on_year_by_ticke = Window.partitionBy(F.year("agent_hired_on")).orderBy(F.col("ticket_opened_at").asc_nulls_last(), "id")
w_ticket_id_by_ticket_requeste_2 = Window.partitionBy("ticket_id").orderBy(F.col("ticket_requester_id").desc(), "id")
w_ticket_id_by_ticket_requeste_3 = Window.partitionBy("ticket_id").orderBy(F.col("ticket_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor = Window.partitionBy("actor")
w_event_type_by_ticket_request = Window.partitionBy("event_type").orderBy(F.col("ticket_requester_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_ticket_request_2 = Window.partitionBy("event_type").orderBy(F.col("ticket_requester_id").asc_nulls_last(), "id")
w_second_ticket_opened_at_to_d = Window.partitionBy(F.to_date("second_ticket_opened_at"), "ticket_opened_at_quartile")
w_ticket_opened_at_quartile_ti = Window.partitionBy("ticket_opened_at_quartile", "ticket_agent_id")

recent_events = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-02-24").cast("date")) | (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched = (
    recent_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.agent_id").isNotNull())
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.max("ticket_requester_id").over(w_ticket_id_by_ticket_requeste).alias("ticket_requester_id_max_2"),
        F.ntile(4).over(w_agent_hired_on_year_by_ticke).alias("ticket_opened_at_quartile"),
        F.round(F.percent_rank().over(w_ticket_id_by_ticket_requeste_2), 3).alias("ticket_requester_id_percent_rank"),
        F.first("ticket_opened_at").over(w_ticket_id_by_ticket_requeste_3).alias("first_ticket_opened_at"),
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_actor), F.lit(0)), 4).alias("ticket_requester_id_share_of_actor"),
        F.nth_value("ticket_opened_at", 2).over(w_event_type_by_ticket_request).alias("second_ticket_opened_at"),
        F.dense_rank().over(w_event_type_by_ticket_request_2).alias("ticket_requester_id_dense_rank"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_actor), 2).alias("ticket_requester_id_vs_actor_avg"),
    )
)

annotated = (
    with_windows
    .filter(F.col("ticket_requester_id_percent_rank") < 0.8)
    .select(
        "*",
        F.concat(F.col("agent_team"), F.lit("#"), F.col("id").cast("string")).alias("agent_team_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("agent_team")), " ", "_")).alias("agent_team_slug"),
    )
)

ranked = (
    annotated
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "ticket_requester_id",
        "ticket_opened_at",
        "agent_team",
        "ticket_opened_at_quartile",
        "ticket_requester_id_percent_rank",
        "first_ticket_opened_at",
        "ticket_requester_id_share_of_actor",
        "ticket_requester_id_dense_rank",
        "ticket_requester_id_vs_actor_avg",
        "agent_team_ref",
        "agent_team_slug",
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_second_ticket_opened_at_to_d), 2).alias("ticket_requester_id_vs_second_ticket_opened_at_t"),
        F.count(F.lit(1)).over(w_second_ticket_opened_at_to_d).alias("rows_in_second_ticket_opened_at_to_date_ticket_o"),
        F.min("ticket_requester_id_max_2").over(w_ticket_opened_at_quartile_ti).alias("ticket_opened_at_quartile_ticket_agent_id_min_ti"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_type",
        "ticket_requester_id",
        "agent_team",
        "ticket_opened_at_quartile",
        "ticket_requester_id_percent_rank",
        "first_ticket_opened_at",
        "ticket_requester_id_share_of_actor",
        "ticket_requester_id_dense_rank",
        "ticket_requester_id_vs_actor_avg",
        "agent_team_ref",
        "agent_team_slug",
        "ticket_requester_id_vs_second_ticket_opened_at_t",
        "rows_in_second_ticket_opened_at_to_date_ticket_o",
        "ticket_opened_at_quartile_ticket_agent_id_min_ti",
        F.coalesce(F.col("agent_team"), F.lit("UNKNOWN")).alias("agent_team_or_unknown"),
    )
    .orderBy(
        F.col("event_type").asc(),
        F.col("ticket_requester_id").desc_nulls_first(),
    )
)
