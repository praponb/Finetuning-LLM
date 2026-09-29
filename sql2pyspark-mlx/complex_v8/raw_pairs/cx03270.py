from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_agent_hired_on = Window.orderBy("agent_hired_on", "id")
w_all_by_agent_hired_on_2 = Window.orderBy(F.col("agent_hired_on").desc(), "id")
w_all_by_agent_hired_on_full = Window.orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_tickets = (
    df_tickets
    .filter((F.col("channel") == "phone") & (F.hour(F.col("opened_at")).between(10, 16)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
    )
)

combined_details_agent = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_details = (
    combined_details_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(~F.col("su.tier").isin("T3", "T1"))
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.agent_tier",
        "s.agent_team",
        "s.agent_hired_on",
        F.col("su.tier").alias("agent_tier_2"),
    )
)

metrics = (
    with_details
    .select(
        "id",
        "agent_id",
        "closed_at",
        "agent_tier",
        "agent_team",
        "agent_hired_on",
        "agent_tier_2",
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_agent_hired_on))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.row_number().over(w_all_by_agent_hired_on_2).alias("seq_no"),
        F.lag("closed_at").over(w_all_by_agent_hired_on_2).alias("prev_closed_at"),
        F.nth_value("closed_at", 2).over(w_all_by_agent_hired_on_full).alias("second_closed_at"),
        F.row_number().over(w_all_by_agent_hired_on).alias("agent_hired_on_seq"),
    )
)

annotated = (
    metrics
    .filter(~F.col("agent_tier").isin("T3", "T2"))
    .select(
        "id",
        "closed_at",
        "agent_tier",
        "agent_team",
        "agent_hired_on",
        "agent_tier_2",
        "minutes_since_prev_closed_at",
        "second_closed_at",
        "agent_hired_on_seq",
        F.date_format(F.col("second_closed_at"), "MMM yyyy").alias("second_closed_at_label"),
    )
)

result = (
    annotated
    .rollup("agent_team", "agent_tier_2", "agent_tier")
    .agg(
        F.grouping("agent_team").alias("is_total_agent_team"),
        F.round(F.sum("minutes_since_prev_closed_at"), 2).alias("total_minutes_since_prev_closed_at"),
        F.min("closed_at").alias("first_closed_at"),
        F.array_sort(F.collect_set("agent_team")).alias("agent_team_list"),
    )
    .orderBy(
        F.col("agent_team").asc_nulls_last(),
        F.col("agent_tier_2").asc_nulls_last(),
        F.col("agent_tier").asc_nulls_last(),
    )
)
