from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel")
w_csat_channel_by_csat_running = Window.partitionBy("csat", "channel").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_closed_at_running = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_channel_by_csat_ties = Window.partitionBy("csat", "channel").orderBy(F.col("csat").desc())
w_csat_channel_by_csat_full = Window.partitionBy("csat", "channel").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")

base_tickets = (
    df_tickets
    .filter((F.col("channel") != "phone") & (F.col("opened_at") < F.lit("2025-02-24 08:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
        F.when(F.col("priority").isin("P3", "P4"), "group_a").when(F.col("priority") == "P2", "group_b").otherwise("other").alias("priority_group"),
    )
)

with_details = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.priority_clean",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

with_trends = (
    with_details
    .select(
        "id",
        "closed_at",
        "csat",
        "tags",
        "priority_clean",
        "agent_name",
        F.round(F.col("csat") - F.avg("csat").over(w_channel), 2).alias("csat_vs_channel_avg"),
        F.sum("csat").over(w_channel).alias("channel_sum_csat"),
        F.sum("csat").over(w_csat_channel_by_csat_running).alias("running_csat"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.first("closed_at", ignorenulls=True).over(w_all_by_closed_at_running).alias("first_known_closed_at"),
        F.round(F.cume_dist().over(w_csat_channel_by_csat_ties), 3).alias("csat_cume_dist"),
        F.first("csat").over(w_csat_channel_by_csat_full).alias("first_csat"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_closed_at))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.nth_value("csat", 2).over(w_csat_channel_by_csat_full).alias("second_csat"),
        F.size(F.collect_set("agent_id").over(w_channel)).alias("distinct_agent_id_in_channel"),
        F.size(F.collect_set("agent_team").over(w_channel)).alias("distinct_agent_team_in_channel"),
    )
)

tagged = (
    with_trends
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("agent_name")), " ", "_")).alias("agent_name_slug"),
        F.greatest(F.col("rows_in_channel"), F.col("csat")).alias("greatest_rows_in_channel_csat"),
    )
)

result = (
    tagged
    .select(
        "id",
        "closed_at",
        "csat",
        "agent_name",
        "running_csat",
        "rows_in_channel",
        "first_known_closed_at",
        "csat_cume_dist",
        "second_csat",
        "distinct_agent_id_in_channel",
        "agent_name_slug",
        "greatest_rows_in_channel_csat",
        F.size(F.col("tags")).alias("tags_count"),
    )
    .orderBy("id")
)
