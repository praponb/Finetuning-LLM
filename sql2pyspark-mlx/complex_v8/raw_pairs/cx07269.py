from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat_by_closed_at_t = Window.partitionBy("agent_id", "csat").orderBy("closed_at")
w_csat_agent_id_by_closed_at = Window.partitionBy("csat", "agent_id").orderBy(F.col("closed_at").desc_nulls_first(), "id")
w_agent_team_csat_by_agent_hir = Window.partitionBy("agent_team", "csat").orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_priority = Window.partitionBy("csat", "priority")
w_csat_agent_team = Window.partitionBy("csat", "agent_team")
w_agent_id_csat_by_closed_at_p = Window.partitionBy("agent_id", "csat").orderBy("closed_at", "id").rowsBetween(-1, -1)
w_agent_team_csat_by_agent_hir_2 = Window.partitionBy("agent_team", "csat").orderBy(F.col("agent_hired_on").desc(), "id")
w_all_by_csat_agent_team_sum_c = Window.orderBy("csat_agent_team_sum_csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_csat_agent_team_sum_c_2 = Window.orderBy("csat_agent_team_sum_csat", "id")
w_agent_id_prev_csat_by_distin = Window.partitionBy("agent_id", "prev_csat").orderBy("distinct_agent_team_in_csat_priority", "id")

scoped = (
    df_tickets
    .filter(F.hour(F.col("opened_at")).between(9, 14))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

with_parent_agent = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.hired_on").between(F.lit("2024-05-02").cast("date"), F.lit("2025-03-30").cast("date")))
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.csat",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

sequenced = (
    with_parent_agent
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "agent_team",
        "agent_name",
        "agent_hired_on",
        F.round(F.cume_dist().over(w_agent_id_csat_by_closed_at_t), 3).alias("closed_at_cume_dist"),
        F.rank().over(w_csat_agent_id_by_closed_at).alias("closed_at_rank"),
        F.nth_value("closed_at", 2).over(w_agent_team_csat_by_agent_hir).alias("second_closed_at"),
        F.size(F.collect_set("agent_team").over(w_csat_priority)).alias("distinct_agent_team_in_csat_priority"),
        F.sum("csat").over(w_csat_agent_team).alias("csat_agent_team_sum_csat"),
        F.count(F.lit(1)).over(w_csat_priority).alias("rows_in_csat_priority"),
        F.max("csat").over(w_agent_id_csat_by_closed_at_p).alias("csat_max_1_prior"),
        F.lag("csat").over(w_agent_team_csat_by_agent_hir_2).alias("prev_csat"),
    )
)

labeled = (
    sequenced
    .filter(F.col("rows_in_csat_priority") > 9)
    .select(
        "*",
        F.to_date(F.col("second_closed_at")).alias("second_closed_at_date"),
    )
)

metrics = (
    labeled
    .select(
        "*",
        F.last("closed_at_rank").over(w_all_by_csat_agent_team_sum_c).alias("last_closed_at_rank"),
        F.lead("closed_at").over(w_all_by_csat_agent_team_sum_c_2).alias("next_closed_at"),
        F.lag("closed_at_rank").over(w_agent_id_prev_csat_by_distin).alias("prev_closed_at_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "priority",
        "agent_team",
        "agent_name",
        "agent_hired_on",
        "closed_at_cume_dist",
        "closed_at_rank",
        "csat_agent_team_sum_csat",
        "rows_in_csat_priority",
        "csat_max_1_prior",
        "prev_csat",
        "last_closed_at_rank",
        "next_closed_at",
        "prev_closed_at_rank",
    )
)
