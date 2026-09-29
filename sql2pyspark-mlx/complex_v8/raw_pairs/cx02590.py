from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_opened_at_year_r = Window.partitionBy("agent_id").orderBy("opened_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_priority = Window.partitionBy("agent_id", "priority")
w_agent_id_by_opened_at_year_f = Window.partitionBy("agent_id").orderBy("opened_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_opened_at_year = Window.partitionBy("agent_id").orderBy("opened_at_year", "id")
w_agent_id_by_opened_at_year_t = Window.partitionBy("agent_id").orderBy("opened_at_year")

filtered_tickets = (
    df_tickets
    .filter(F.col("requester_id") > 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 3600, 1).alias("hours_closed_at_to_opened_at"),
        F.year(F.col("opened_at")).alias("opened_at_year"),
    )
)

with_parent = (
    filtered_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

windowed = (
    with_parent
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "greatest_requester_id_csat",
        "hours_closed_at_to_opened_at",
        "opened_at_year",
        "agent_tier",
        "agent_team",
        F.last("hours_closed_at_to_opened_at", ignorenulls=True).over(w_agent_id_by_opened_at_year_r).alias("last_known_hours_closed_at_to_opened_at"),
        F.min("hours_closed_at_to_opened_at").over(w_agent_id_priority).alias("agent_id_priority_min_hours_closed_at_to_opened_"),
        F.last("opened_at_year").over(w_agent_id_by_opened_at_year_f).alias("last_opened_at_year"),
        F.round(F.cume_dist().over(w_agent_id_by_opened_at_year), 3).alias("opened_at_year_cume_dist"),
        F.max("requester_id").over(w_agent_id_priority).alias("agent_id_priority_max_requester_id"),
        F.round(F.percent_rank().over(w_agent_id_by_opened_at_year_t), 3).alias("opened_at_year_percent_rank"),
    )
)

agent_tier_stats = (
    windowed
    .groupBy("agent_tier")
    .agg(F.percentile_approx("agent_id_priority_min_hours_closed_at_to_opened_", 0.5).alias("median_agent_id_priority_min_hours_closed_at_to_"))
)

above_average = (
    windowed.alias("s")
    .join(agent_tier_stats.alias("g"), F.col("g.agent_tier") == F.col("s.agent_tier"), "inner")
    .filter(F.col("s.agent_id_priority_min_hours_closed_at_to_opened_") >= F.col("g.median_agent_id_priority_min_hours_closed_at_to_"))
    .select(
        "s.*",
        "g.median_agent_id_priority_min_hours_closed_at_to_",
        F.round(F.col("s.agent_id_priority_min_hours_closed_at_to_opened_") - F.col("g.median_agent_id_priority_min_hours_closed_at_to_"), 2).alias("agent_id_priority_min_hours_closed_at_to_opened__2"),
    )
)

derived = (
    above_average
    .select(
        "*",
        F.coalesce(F.col("opened_at_year_cume_dist"), F.lit(0.0)).alias("opened_at_year_cume_dist_filled"),
        F.concat(F.col("channel"), F.lit("#"), F.col("agent_id").cast("string")).alias("channel_ref"),
    )
)

result = (
    derived
    .filter(F.col("hours_closed_at_to_opened_at") < 1200.0)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "hours_closed_at_to_opened_at",
        "opened_at_year",
        "agent_tier",
        "agent_team",
        "last_known_hours_closed_at_to_opened_at",
        "agent_id_priority_min_hours_closed_at_to_opened_",
        "last_opened_at_year",
        "agent_id_priority_max_requester_id",
        "opened_at_year_percent_rank",
        "agent_id_priority_min_hours_closed_at_to_opened__2",
        "opened_at_year_cume_dist_filled",
        "channel_ref",
        F.date_format(F.col("closed_at"), "yyyy-MM-dd HH:00").alias("closed_at_label"),
        F.concat(F.col("priority"), F.lit("#"), F.col("id").cast("string")).alias("priority_ref"),
    )
)
