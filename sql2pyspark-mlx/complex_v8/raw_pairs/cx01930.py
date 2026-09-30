from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_agent_id = Window.partitionBy("csat", "agent_id")
w_agent_id_by_csat = Window.partitionBy("agent_id").orderBy("csat", "id")
w_csat_agent_team_by_csat_runn = Window.partitionBy("csat", "agent_team").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_agent_team_by_csat_ties = Window.partitionBy("csat", "agent_team").orderBy("csat")
w_agent_id_by_csat_running = Window.partitionBy("agent_id").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_next_csat_by_csat_runni = Window.partitionBy("csat", "next_csat").orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat = Window.partitionBy("csat")
w_all_by_opened_at_ties = Window.orderBy(F.col("opened_at").desc_nulls_first())

cleaned_tickets = (
    df_tickets
    .filter(F.col("requester_id") <= 22)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        F.when(F.col("priority").isin("P4", "P1"), "group_a").when(F.col("priority") == "P3", "group_b").otherwise("other").alias("priority_group"),
    )
)

with_details_agent = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.csat",
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

metrics = (
    with_details_agent
    .select(
        "*",
        F.round(F.col("csat") - F.avg("csat").over(w_csat_agent_id), 2).alias("csat_vs_csat_agent_id_avg"),
        F.lead("csat").over(w_agent_id_by_csat).alias("next_csat"),
        F.sum("csat").over(w_csat_agent_team_by_csat_runn).alias("running_csat"),
        F.round(F.cume_dist().over(w_csat_agent_team_by_csat_ties), 3).alias("csat_cume_dist"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_agent_id_by_csat))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.sum("csat").over(w_agent_id_by_csat_running).alias("running_csat_2"),
        F.size(F.collect_set("agent_tier").over(w_csat_agent_id)).alias("distinct_agent_tier_in_csat_agent_id"),
        F.first("opened_at", ignorenulls=True).over(w_agent_id_by_csat_running).alias("first_known_opened_at"),
    )
)

labeled = (
    metrics
    .select(
        "id",
        "opened_at",
        "csat",
        "agent_name",
        "csat_vs_csat_agent_id_avg",
        "next_csat",
        "running_csat",
        "minutes_since_prev_opened_at",
        "distinct_agent_tier_in_csat_agent_id",
        "first_known_opened_at",
        F.substring(F.trim(F.col("agent_team")), 1, 3).alias("agent_team_prefix"),
    )
)

with_windows = (
    labeled
    .select(
        "*",
        F.max("csat").over(w_csat_next_csat_by_csat_runni).alias("max_csat_so_far"),
        F.max("csat_vs_csat_agent_id_avg").over(w_csat).alias("csat_max_csat_vs_csat_agent_id_avg"),
        F.round(F.cume_dist().over(w_all_by_opened_at_ties), 3).alias("opened_at_cume_dist"),
        F.sum("csat_vs_csat_agent_id_avg").over(w_csat_next_csat_by_csat_runni).alias("running_csat_vs_csat_agent_id_avg"),
        F.max("csat_vs_csat_agent_id_avg").over(w_csat_next_csat_by_csat_runni).alias("max_csat_vs_csat_agent_id_avg_so_far"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "csat",
        "agent_name",
        "csat_vs_csat_agent_id_avg",
        "next_csat",
        "running_csat",
        "minutes_since_prev_opened_at",
        "distinct_agent_tier_in_csat_agent_id",
        "first_known_opened_at",
        "agent_team_prefix",
        "max_csat_so_far",
        "running_csat_vs_csat_agent_id_avg",
        "max_csat_vs_csat_agent_id_avg_so_far",
        (F.col("csat") + F.col("next_csat")).alias("csat_plus_next_csat"),
    )
    .orderBy(F.col("running_csat").desc())
)
