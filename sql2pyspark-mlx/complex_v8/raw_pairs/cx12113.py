from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_team_agent_id_by_days = Window.partitionBy("agent_team", "agent_id").orderBy("days_since_opened_at", "id")
w_agent_id_agent_team = Window.partitionBy("agent_id", "agent_team")
w_all_by_opened_at_full = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_opened_at_month_by = Window.partitionBy("agent_id", F.date_trunc("month", "opened_at")).orderBy("days_since_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id = Window.partitionBy("agent_id")
w_agent_id_agent_team_by_opene = Window.partitionBy("agent_id", "agent_team").orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_agent_team_by_opened_at = Window.partitionBy("agent_team").orderBy("opened_at", "id")
w_agent_team_agent_id_by_days_2 = Window.partitionBy("agent_team", "agent_id").orderBy("days_since_opened_at")
w_agent_team_by_opened_at_ties = Window.partitionBy("agent_team").orderBy("opened_at")

scoped_tickets = (
    df_tickets
    .filter((F.col("requester_id") >= 7) & (~F.col("channel").isin("chat", "web")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "csat",
        "tags",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

enriched = (
    scoped_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.tags",
        "s.days_since_opened_at",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

metrics = (
    enriched
    .select(
        "id",
        "agent_id",
        "opened_at",
        "tags",
        "days_since_opened_at",
        "agent_team",
        "agent_name",
        F.rank().over(w_agent_team_agent_id_by_days).alias("days_since_opened_at_rank"),
        F.first("days_since_opened_at").over(w_all_by_opened_at_full).alias("first_days_since_opened_at"),
        F.nth_value("opened_at", 2).over(w_agent_id_opened_at_month_by).alias("second_opened_at"),
        F.round(F.percent_rank().over(w_agent_team_agent_id_by_days), 3).alias("days_since_opened_at_percent_rank"),
        F.sum("days_since_opened_at").over(w_agent_id).alias("agent_id_sum_days_since_opened_at"),
        F.round(F.avg("days_since_opened_at").over(w_agent_id_agent_team_by_opene), 2).alias("days_since_opened_at_avg_2"),
        F.ntile(5).over(w_agent_team_agent_id_by_days).alias("days_since_opened_at_quintile"),
        F.size(F.collect_set("agent_team").over(w_agent_id)).alias("distinct_agent_team_in_agent_id"),
        F.round(F.col("days_since_opened_at") / F.nullif(F.sum("days_since_opened_at").over(w_agent_id_agent_team), F.lit(0)), 4).alias("days_since_opened_at_share_of_agent_id_agent_tea"),
        F.lead("days_since_opened_at").over(w_agent_team_agent_id_by_days).alias("next_days_since_opened_at"),
        F.round(F.percent_rank().over(w_agent_team_by_opened_at), 3).alias("opened_at_percent_rank"),
        F.round(F.percent_rank().over(w_agent_team_agent_id_by_days_2), 3).alias("days_since_opened_at_percent_rank_2"),
        F.round(F.cume_dist().over(w_agent_team_by_opened_at_ties), 3).alias("opened_at_cume_dist"),
    )
)

tagged = (
    metrics
    .select(
        "id",
        "agent_id",
        "opened_at",
        "tags",
        "agent_name",
        "first_days_since_opened_at",
        "second_opened_at",
        "days_since_opened_at_percent_rank",
        "days_since_opened_at_avg_2",
        "days_since_opened_at_quintile",
        "next_days_since_opened_at",
        "opened_at_percent_rank",
        "days_since_opened_at_percent_rank_2",
        "opened_at_cume_dist",
        F.coalesce(F.col("agent_team"), F.lit("UNKNOWN")).alias("agent_team_or_unknown"),
        (F.col("opened_at") + F.expr("INTERVAL 30 MINUTES")).alias("opened_at_plus_30m"),
        F.when(F.col("days_since_opened_at") >= 420, "A").when(F.col("days_since_opened_at") >= 210, "B").otherwise("C").alias("days_since_opened_at_tier"),
    )
)

per_group = (
    tagged
    .groupBy(
        F.date_trunc("month", "opened_at").alias("opened_at_month"),
        F.date_trunc("month", "opened_at_plus_30m").alias("opened_at_plus_30m_month"),
        "days_since_opened_at_quintile",
    )
    .agg(
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.length(F.trim(F.col("agent_name"))) >= 7, F.col("days_since_opened_at_quintile")).otherwise(0)).alias("days_since_opened_at_quintile_length_trim_agent_"),
        F.countDistinct("agent_team_or_unknown").alias("distinct_agent_team_or_unknowns"),
    )
)

result = (
    per_group
    .select(
        "opened_at_month",
        "opened_at_plus_30m_month",
        "agent_name_list",
        "distinct_ids",
        "cnt",
        "days_since_opened_at_quintile_length_trim_agent_",
        "distinct_agent_team_or_unknowns",
        F.coalesce(F.col("cnt"), F.lit(0)).alias("cnt_filled"),
    )
    .orderBy(F.col("distinct_agent_team_or_unknowns").asc())
)
