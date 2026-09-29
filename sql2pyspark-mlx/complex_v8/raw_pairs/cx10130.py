from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_closed_at_dayofweek = Window.partitionBy("agent_id", "closed_at_dayofweek")
w_closed_at_dayofweek_opened_a = Window.partitionBy("closed_at_dayofweek", F.to_date("opened_at")).orderBy(F.col("closed_at_dayofweek").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_tickets
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "tags",
        F.concat_ws(" / ", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
        F.least(F.col("requester_id"), F.col("csat")).alias("least_requester_id_csat"),
        F.dayofweek(F.col("closed_at")).alias("closed_at_dayofweek"),
    )
)

ranked = (
    scoped
    .select(
        "*",
        F.sum("requester_id").over(w_agent_id_closed_at_dayofweek).alias("agent_id_closed_at_dayofweek_sum_requester_id"),
        F.nth_value("closed_at_dayofweek", 2).over(w_closed_at_dayofweek_opened_a).alias("second_closed_at_dayofweek"),
        F.first("opened_at").over(w_closed_at_dayofweek_opened_a).alias("first_opened_at"),
        F.round(F.col("closed_at_dayofweek") - F.avg("closed_at_dayofweek").over(w_agent_id_closed_at_dayofweek), 2).alias("closed_at_dayofweek_vs_agent_id_closed_at_dayofw"),
    )
)

with_parent_agent = (
    ranked.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(~F.col("su.team").isin("Tech", "Billing"))
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.tags",
        "s.priority_channel_label",
        "s.closed_at_dayofweek",
        "s.agent_id_closed_at_dayofweek_sum_requester_id",
        "s.second_closed_at_dayofweek",
        "s.first_opened_at",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

annotated = (
    with_parent_agent
    .filter(F.col("channel") != "phone")
    .select(
        "*",
        (F.col("requester_id") - F.col("closed_at_dayofweek")).alias("requester_id_minus_closed_at_dayofweek"),
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
    )
)

result = (
    annotated
    .cube("closed_at_dayofweek", "agent_id", "agent_tier")
    .agg(
        F.grouping("closed_at_dayofweek").alias("is_total_closed_at_dayofweek"),
        F.sum(F.when(F.col("agent_tier").isin("T1", "T3"), F.col("agent_id_closed_at_dayofweek_sum_requester_id"))).alias("agent_id_closed_at_dayofweek_sum_requester_id_ag"),
    )
    .select(
        "closed_at_dayofweek",
        "agent_id",
        F.coalesce(F.col("agent_tier"), F.lit("ALL")).alias("agent_tier"),
        "is_total_closed_at_dayofweek",
        "agent_id_closed_at_dayofweek_sum_requester_id_ag",
    )
    .orderBy(
        F.col("closed_at_dayofweek").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
        F.col("agent_tier").asc_nulls_last(),
    )
)
