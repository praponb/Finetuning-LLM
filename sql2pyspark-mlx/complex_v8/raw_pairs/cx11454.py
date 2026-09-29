from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_agent_team_priority_by_csat = Window.partitionBy("agent_team", "priority").orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_agent_team_priority_by_csat_2 = Window.partitionBy("agent_team", "priority").orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_team_priority_by_csat_3 = Window.partitionBy("agent_team", "priority").orderBy(F.col("csat").asc_nulls_last(), "id")

valid_tickets = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "refund")) | (F.col("requester_id") > 8))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
    )
)

with_details_agent = (
    valid_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.csat",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_trends = (
    with_details_agent
    .select(
        "*",
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.round(F.avg("csat").over(w_agent_team_priority_by_csat), 2).alias("csat_avg_7"),
        F.size(F.collect_set("priority").over(w_agent_id)).alias("distinct_priority_in_agent_id"),
        F.nth_value("csat", 2).over(w_agent_team_priority_by_csat_2).alias("second_csat"),
        F.dense_rank().over(w_agent_team_priority_by_csat_3).alias("csat_dense_rank"),
        F.rank().over(w_agent_team_priority_by_csat_3).alias("csat_rank"),
    )
)

tagged = (
    with_trends
    .select(
        "*",
        F.when(F.col("priority").isin("P2", "P4"), "group_a").when(F.col("priority") == "P3", "group_b").otherwise("other").alias("priority_group"),
    )
)

result = (
    tagged
    .select("id", "csat_dense_rank", "distinct_priority_in_agent_id")
    .unpivot(
        ids=["id"],
        values=["csat_dense_rank", "distinct_priority_in_agent_id"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .orderBy("id", "metric_name")
)
