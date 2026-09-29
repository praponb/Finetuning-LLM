from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_team = Window.partitionBy("agent_team")
w_agent_team_agent_hired_on_ye = Window.partitionBy("agent_team", F.year("agent_hired_on")).orderBy(F.col("requester_id").desc(), "id").rowsBetween(-3, Window.currentRow)
w_agent_team_closed_at_month = Window.partitionBy("agent_team", F.month("closed_at"))
w_agent_team_agent_hired_on_ye_2 = Window.partitionBy("agent_team", F.year("agent_hired_on")).orderBy(F.col("requester_id").desc(), "id")
w_agent_team_by_agent_hired_on = Window.partitionBy("agent_team").orderBy("agent_hired_on", "id").rowsBetween(-6, Window.currentRow)
w_all_by_requester_id = Window.orderBy(F.col("requester_id").asc_nulls_last(), "id")

cleaned = (
    df_tickets
    .filter((F.col("csat") <= 4) | (F.col("requester_id") >= 7))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        F.concat(F.col("priority"), F.lit("#"), F.col("agent_id").cast("string")).alias("priority_ref"),
    )
)

combined_details_agent = (
    cleaned.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority_ref",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

sequenced = (
    combined_details_agent
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority_ref",
        "agent_hired_on",
        F.size(F.collect_set("agent_id").over(w_agent_team)).alias("distinct_agent_id_in_agent_team"),
        F.sum("requester_id").over(w_agent_team_agent_hired_on_ye).alias("requester_id_sum_4"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_team_closed_at_month), 2).alias("requester_id_vs_agent_team_closed_at_month_avg"),
        F.min("requester_id").over(w_agent_team).alias("agent_team_min_requester_id"),
        F.dense_rank().over(w_agent_team_agent_hired_on_ye_2).alias("requester_id_dense_rank"),
        F.round(F.avg("requester_id").over(w_agent_team_by_agent_hired_on), 2).alias("requester_id_avg_7"),
        F.ntile(4).over(w_all_by_requester_id).alias("requester_id_quartile"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "closed_at",
        "priority_ref",
        "agent_hired_on",
        "distinct_agent_id_in_agent_team",
        "requester_id_vs_agent_team_closed_at_month_avg",
        "agent_team_min_requester_id",
        "requester_id_dense_rank",
        "requester_id_avg_7",
        "requester_id_quartile",
    )
    .orderBy(
        F.col("requester_id_avg_7").desc_nulls_first(),
        F.col("requester_id_quartile").asc(),
    )
)
