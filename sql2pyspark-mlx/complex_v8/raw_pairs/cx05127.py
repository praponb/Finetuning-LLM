from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_event_at_year_by = Window.partitionBy("event_type", "event_at_year").orderBy(F.col("event_at_year").desc(), "id").rowsBetween(-3, Window.currentRow)
w_actor_ticket_id = Window.partitionBy("actor", "ticket_id")
w_event_at_year_by_event_at_ye = Window.partitionBy("event_at_year").orderBy(F.col("event_at_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_event_at_year_by_2 = Window.partitionBy("event_type", "event_at_year").orderBy(F.col("event_at_year").desc(), "id").rowsBetween(-6, -1)
w_event_type_event_at_year_by_3 = Window.partitionBy("event_type", "event_at_year").orderBy(F.col("event_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_event_types_min_tic = Window.partitionBy("distinct_event_types", "min_ticket_csat").orderBy("row_count", "distinct_event_type_in_actor_ticket_id", "agent_team", "event_at_month")
w_distinct_event_types_min_tic_2 = Window.partitionBy("distinct_event_types", "min_ticket_csat").orderBy("row_count")
w_distinct_event_type_in_actor = Window.partitionBy("distinct_event_type_in_actor_ticket_id")

base = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(7, 9)) | (F.col("actor").isin("customer", "agent")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.year(F.col("event_at")).alias("event_at_year"),
    )
)

with_details = (
    base.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.array_contains(F.col("t.tags"), "upgrade")), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_year",
        F.col("t.csat").alias("ticket_csat"),
        F.col("su.team").alias("agent_team"),
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.sum("ticket_csat").over(w_event_type_event_at_year_by).alias("ticket_csat_sum_4"),
        F.size(F.collect_set("event_type").over(w_actor_ticket_id)).alias("distinct_event_type_in_actor_ticket_id"),
        F.count(F.lit(1)).over(w_actor_ticket_id).alias("rows_in_actor_ticket_id"),
        F.last("ticket_csat").over(w_event_at_year_by_event_at_ye).alias("last_ticket_csat"),
        F.round(F.avg("ticket_csat").over(w_event_type_event_at_year_by_2), 2).alias("ticket_csat_avg_6_prior"),
        F.last("ticket_csat", ignorenulls=True).over(w_event_type_event_at_year_by_3).alias("last_known_ticket_csat"),
    )
)

per_group = (
    with_windows
    .groupBy(
        "distinct_event_type_in_actor_ticket_id",
        "agent_team",
        F.date_trunc("month", "event_at").alias("event_at_month"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.min("ticket_csat").alias("min_ticket_csat"),
        F.countDistinct("agent_team").alias("distinct_agent_teams"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.sum(F.when(~F.col("event_type").isin("escalated", "resolved"), F.col("last_known_ticket_csat"))).alias("last_known_ticket_csat_event_type_not_in_escalat"),
    )
)

filtered = (
    per_group
    .crossJoin(per_group.agg(F.avg("last_known_ticket_csat_event_type_not_in_escalat").alias("overall_avg_last_known_ticket_csat_event_type_no")))
    .filter(F.col("last_known_ticket_csat_event_type_not_in_escalat") > F.col("overall_avg_last_known_ticket_csat_event_type_no"))
    .drop("overall_avg_last_known_ticket_csat_event_type_no")
)

windowed = (
    filtered
    .select(
        "distinct_event_type_in_actor_ticket_id",
        "agent_team",
        "event_at_month",
        "row_count",
        "min_ticket_csat",
        "distinct_agent_teams",
        "distinct_event_types",
        F.row_number().over(w_distinct_event_types_min_tic).alias("rn"),
        F.rank().over(w_distinct_event_types_min_tic_2).alias("row_count_rank"),
        F.size(F.collect_set("agent_team").over(w_distinct_event_type_in_actor)).alias("distinct_agent_team_in_distinct_event_type_in_ac"),
        F.round(F.avg("distinct_event_types").over(w_distinct_event_type_in_actor), 2).alias("distinct_event_type_in_actor_ticket_id_avg_disti"),
    )
)

result = (
    windowed
    .select(
        "distinct_event_type_in_actor_ticket_id",
        "agent_team",
        "event_at_month",
        "row_count",
        "distinct_event_types",
        "row_count_rank",
        "distinct_agent_team_in_distinct_event_type_in_ac",
        F.concat(F.col("agent_team"), F.lit("#"), F.col("row_count").cast("string")).alias("agent_team_ref"),
        F.when(F.col("agent_team").isin("Billing", "VIP"), "group_a").when(F.col("agent_team") == "Tech", "group_b").otherwise("other").alias("agent_team_group"),
    )
)
