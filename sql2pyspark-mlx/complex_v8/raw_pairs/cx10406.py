from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_opened_at = Window.partitionBy("agent_id").orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_agent_id_by_opened_at_full = Window.partitionBy("agent_id").orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_opened_at_runnin = Window.partitionBy("agent_id").orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tickets = (
    df_tickets
    .filter((F.hour(F.col("closed_at")).between(7, 11)) & (F.col("csat") >= 3))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "csat",
        "tags",
    )
)

ranked = (
    valid_tickets
    .select(
        "*",
        F.rank().over(w_agent_id_by_opened_at).alias("opened_at_rank"),
        F.nth_value("csat", 2).over(w_agent_id_by_opened_at_full).alias("second_csat"),
        F.sum("csat").over(w_agent_id_by_opened_at_runnin).alias("running_csat"),
    )
)

with_parent = (
    ranked.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.csat",
        "s.tags",
        "s.opened_at_rank",
        "s.second_csat",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

stats = (
    with_parent
    .filter(F.array_contains(F.col("tags"), "login"))
    .groupBy(
        "second_csat",
        F.date_trunc("month", "opened_at").alias("opened_at_month"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("agent_team") == "Onboarding", 1)) / F.count("*"), 1).alias("pct_agent_team_onboarding"),
        F.sum(F.when(F.lower(F.col("agent_name")).like("%t %"), F.col("csat"))).alias("csat_lower_agent_name_like_t"),
        F.max("opened_at").alias("last_opened_at"),
        F.min("opened_at_rank").alias("min_opened_at_rank"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

annotated = (
    stats
    .select(
        "second_csat",
        "opened_at_month",
        "csat_lower_agent_name_like_t",
        "last_opened_at",
        "min_opened_at_rank",
        F.round(F.col("min_opened_at_rank") / F.nullif(F.col("second_csat"), F.lit(0)), 3).alias("min_opened_at_rank_per_second_csat"),
        F.coalesce(F.col("min_opened_at_rank"), F.lit(0)).alias("min_opened_at_rank_filled"),
    )
)

tagged = (
    annotated
    .select(
        "*",
        F.to_date(F.col("opened_at_month")).alias("opened_at_month_date"),
        F.date_trunc("year", F.col("last_opened_at")).alias("last_opened_at_year"),
    )
)

labeled = (
    tagged
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("last_opened_at_year")) - F.unix_timestamp(F.col("last_opened_at"))) / 3600, 1).alias("hours_last_opened_at_to_last_opened_at_year"),
        F.when(F.col("min_opened_at_rank") >= 9, "large").when(F.col("min_opened_at_rank") >= 3, "medium").otherwise("small").alias("min_opened_at_rank_tier"),
        F.coalesce(F.col("min_opened_at_rank"), F.lit(0)).alias("min_opened_at_rank_filled_2"),
    )
)

result = (
    labeled
    .filter(F.col("second_csat") <= 4)
    .select(
        "second_csat",
        "csat_lower_agent_name_like_t",
        "min_opened_at_rank",
        "min_opened_at_rank_filled",
        "hours_last_opened_at_to_last_opened_at_year",
        "min_opened_at_rank_tier",
        "min_opened_at_rank_filled_2",
        F.when(F.col("min_opened_at_rank_tier").isin("medium", "small"), "group_a").when(F.col("min_opened_at_rank_tier") == "large", "group_b").otherwise("other").alias("min_opened_at_rank_tier_group"),
        F.date_format(F.col("opened_at_month"), "yyyy-MM-dd HH:00").alias("opened_at_month_label"),
    )
    .orderBy("min_opened_at_rank", F.col("second_csat").desc_nulls_first())
)
