from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_team = Window.partitionBy("agent_team")
w_channel_agent_id_by_closed_a = Window.partitionBy("channel", "agent_id").orderBy(F.col("closed_at").desc(), "id")
w_channel_agent_id_by_closed_a_2 = Window.partitionBy("channel", "agent_id").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_agent_id_by_closed_a_3 = Window.partitionBy("channel", "agent_id").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_priority_group = Window.partitionBy("csat", "priority_group")
w_all_by_second_days_since_ope = Window.orderBy(F.col("second_days_since_opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_second_days_since_ope_2 = Window.orderBy(F.col("second_days_since_opened_at").desc(), "id")
w_second_days_since_opened_at = Window.partitionBy("second_days_since_opened_at_tercile")
w_second_days_since_opened_at_2 = Window.partitionBy("second_days_since_opened_at_tercile").orderBy(F.col("first_closed_at").desc_nulls_first())
w_second_days_since_opened_at_3 = Window.partitionBy("second_days_since_opened_at_tercile").orderBy(F.col("first_closed_at").desc_nulls_first(), "second_days_since_opened_at_tercile").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_second_days_since_opened_at_4 = Window.partitionBy("second_days_since_opened_at_tercile").orderBy(F.col("first_closed_at").desc_nulls_first(), "second_days_since_opened_at_tercile")

filtered = (
    df_tickets
    .filter((F.col("requester_id") > 8) | (F.col("priority") == "P2"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.when(F.col("priority").isin("P1", "P2"), "group_a").when(F.col("priority") == "P4", "group_b").otherwise("other").alias("priority_group"),
    )
)

with_details_agent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.days_since_opened_at",
        "s.priority_group",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

with_trends = (
    with_details_agent
    .select(
        "id",
        "closed_at",
        "csat",
        "days_since_opened_at",
        "priority_group",
        "agent_name",
        "agent_team",
        F.round(F.avg("days_since_opened_at").over(w_agent_team), 2).alias("agent_team_avg_days_since_opened_at"),
        F.sum("csat").over(w_agent_team).alias("agent_team_sum_csat"),
        F.lead("days_since_opened_at").over(w_channel_agent_id_by_closed_a).alias("next_days_since_opened_at"),
        F.round(F.col("days_since_opened_at") / F.nullif(F.sum("days_since_opened_at").over(w_agent_team), F.lit(0)), 4).alias("days_since_opened_at_share_of_agent_team"),
        F.row_number().over(w_channel_agent_id_by_closed_a).alias("seq_no"),
        F.count(F.lit(1)).over(w_channel_agent_id_by_closed_a_2).alias("running_count"),
        F.round(F.col("days_since_opened_at") - F.avg("days_since_opened_at").over(w_agent_team), 2).alias("days_since_opened_at_vs_agent_team_avg"),
        F.size(F.collect_set("priority_group").over(w_agent_team)).alias("distinct_priority_group_in_agent_team"),
        F.nth_value("days_since_opened_at", 2).over(w_channel_agent_id_by_closed_a_3).alias("second_days_since_opened_at"),
        F.size(F.collect_set("channel").over(w_agent_team)).alias("distinct_channel_in_agent_team"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "*",
        F.count(F.lit(1)).over(w_csat_priority_group).alias("rows_in_csat_priority_group"),
        F.first("seq_no", ignorenulls=True).over(w_all_by_second_days_since_ope).alias("first_known_seq_no"),
        F.ntile(3).over(w_all_by_second_days_since_ope_2).alias("second_days_since_opened_at_tercile"),
        F.ntile(10).over(w_all_by_second_days_since_ope_2).alias("second_days_since_opened_at_decile"),
    )
)

grouped = (
    with_trends_2
    .groupBy("second_days_since_opened_at_tercile")
    .agg(
        F.sum("days_since_opened_at").alias("total_days_since_opened_at"),
        F.round(F.sum("days_since_opened_at_share_of_agent_team") / F.nullif(F.sum("days_since_opened_at"), F.lit(0)), 3).alias("days_since_opened_at_share_of_agent_team_to_days"),
        F.round(F.sum("days_since_opened_at_share_of_agent_team") / F.nullif(F.sum("csat"), F.lit(0)), 3).alias("days_since_opened_at_share_of_agent_team_to_csat"),
        F.min("closed_at").alias("first_closed_at"),
    )
)

windowed = (
    grouped
    .select(
        "*",
        F.round(F.cume_dist().over(w_second_days_since_opened_at_2), 3).alias("first_closed_at_cume_dist"),
        F.min("total_days_since_opened_at").over(w_second_days_since_opened_at_3).alias("min_total_days_since_opened_at_so_far"),
        F.round(F.col("total_days_since_opened_at") / F.nullif(F.sum("total_days_since_opened_at").over(w_second_days_since_opened_at), F.lit(0)), 4).alias("total_days_since_opened_at_share_of_second_days_"),
        F.ntile(4).over(w_second_days_since_opened_at_4).alias("first_closed_at_quartile"),
    )
)

result = (
    windowed
    .select(
        "second_days_since_opened_at_tercile",
        "days_since_opened_at_share_of_agent_team_to_days",
        "first_closed_at",
        "min_total_days_since_opened_at_so_far",
        "total_days_since_opened_at_share_of_second_days_",
        "first_closed_at_quartile",
        F.abs(F.col("first_closed_at_quartile") - 5).alias("first_closed_at_quartile_distance"),
        F.when(F.col("first_closed_at_quartile") >= 7, "HIGH").when(F.col("first_closed_at_quartile") >= 4, "MEDIUM").otherwise("LOW").alias("first_closed_at_quartile_tier"),
    )
)
