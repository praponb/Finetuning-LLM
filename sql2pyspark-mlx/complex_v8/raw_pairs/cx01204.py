from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_tier_by_event_at = Window.partitionBy("agent_tier").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_moving_3 = Window.orderBy("event_at", "id").rowsBetween(-3, Window.currentRow)
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_tier_by_event_at_runni = Window.partitionBy("agent_tier").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type = Window.partitionBy("event_type")
w_ticket_id_event_type = Window.partitionBy("ticket_id", "event_type")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_event_at_seq_distinct_ticket = Window.partitionBy("event_at_seq", "distinct_ticket_id_in_event_type").orderBy(F.col("event_at").desc())
w_all_by_ticket_requester_id_s = Window.orderBy(F.col("ticket_requester_id_share_of_ticket_id_event_typ").desc_nulls_first(), "id")
w_event_at_dense_rank_distinct = Window.partitionBy("event_at_dense_rank", "distinct_ticket_id_in_event_type")
w_all_by_next_ticket_requester = Window.orderBy(F.col("next_ticket_requester_id").desc(), "id").rowsBetween(-6, -1)

cleaned = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")) | (F.col("actor").isin("system", "agent")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

with_parent = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

ranked = (
    with_parent
    .select(
        "id",
        "event_at",
        "event_type",
        "agent_name",
        F.row_number().over(w_agent_tier_by_event_at).alias("event_at_seq"),
        F.lead("ticket_requester_id").over(w_agent_tier_by_event_at).alias("next_ticket_requester_id"),
        F.max("ticket_requester_id").over(w_all_by_event_at_moving_3).alias("ticket_requester_id_max_4"),
        F.first("ticket_requester_id", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_ticket_requester_id"),
        F.max("ticket_requester_id").over(w_agent_tier_by_event_at_runni).alias("max_ticket_requester_id_so_far"),
        F.size(F.collect_set("ticket_id").over(w_event_type)).alias("distinct_ticket_id_in_event_type"),
        F.max("ticket_requester_id").over(w_ticket_id_event_type).alias("ticket_id_event_type_max_ticket_requester_id"),
        F.dense_rank().over(w_all_by_event_at).alias("event_at_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_ticket_id_event_type), F.lit(0)), 4).alias("ticket_requester_id_share_of_ticket_id_event_typ"),
    )
)

windowed = (
    ranked
    .select(
        "*",
        F.round(F.percent_rank().over(w_event_at_seq_distinct_ticket), 3).alias("event_at_percent_rank_2"),
        F.lag("ticket_requester_id_max_4").over(w_all_by_ticket_requester_id_s).alias("prev_ticket_requester_id_max_4"),
        F.size(F.collect_set("event_type").over(w_event_at_dense_rank_distinct)).alias("distinct_event_type_in_event_at_dense_rank_disti"),
        F.lag("event_at", 2).over(w_all_by_ticket_requester_id_s).alias("prev2_event_at"),
        F.max("event_at_percent_rank").over(w_all_by_next_ticket_requester).alias("event_at_percent_rank_max_6_prior"),
        F.round(F.col("ticket_requester_id_max_4") / F.nullif(F.sum("ticket_requester_id_max_4").over(w_event_at_dense_rank_distinct), F.lit(0)), 4).alias("ticket_requester_id_max_4_share_of_event_at_dens"),
    )
)

result = (
    windowed
    .select(
        "id",
        "event_type",
        "agent_name",
        "event_at_seq",
        "next_ticket_requester_id",
        "first_known_ticket_requester_id",
        "max_ticket_requester_id_so_far",
        "distinct_ticket_id_in_event_type",
        "event_at_dense_rank",
        "event_at_percent_rank",
        "ticket_requester_id_share_of_ticket_id_event_typ",
        "event_at_percent_rank_2",
        "prev_ticket_requester_id_max_4",
        "ticket_requester_id_max_4_share_of_event_at_dens",
    )
    .orderBy(
        F.col("ticket_requester_id_share_of_ticket_id_event_typ").asc_nulls_last(),
    )
)
