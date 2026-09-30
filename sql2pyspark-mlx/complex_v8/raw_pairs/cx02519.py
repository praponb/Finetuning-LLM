from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_at_full = Window.orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_priority_by_opened = Window.partitionBy("agent_id", "priority").orderBy("opened_at", "id").rowsBetween(-6, Window.currentRow)
w_all_by_opened_at_ties = Window.orderBy("opened_at")
w_agent_id_priority_by_opened_2 = Window.partitionBy("agent_id", "priority").orderBy("opened_at")
w_agent_id_priority_by_opened_3 = Window.partitionBy("agent_id", "priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_agent_team = Window.partitionBy("priority", "agent_team")
w_agent_id_priority_by_opened_4 = Window.partitionBy("agent_id", "priority").orderBy("opened_at", "id")
w_agent_team_agent_id_by_opene = Window.partitionBy("agent_team", "agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_tickets
    .filter((F.col("opened_at") >= F.lit("2025-01-13 09:00:00").cast("timestamp")) & (F.col("channel") == "phone"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
    )
)

joined_agent = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", "su.agent_name", F.col("su.team").alias("agent_team"))
)

ranked = (
    joined_agent
    .select(
        "*",
        F.first("requester_id").over(w_all_by_opened_at_full).alias("first_requester_id"),
        F.round(F.avg("requester_id").over(w_agent_id_priority_by_opened), 2).alias("requester_id_avg_7"),
        F.round(F.percent_rank().over(w_all_by_opened_at_ties), 3).alias("opened_at_percent_rank"),
        F.dense_rank().over(w_agent_id_priority_by_opened_2).alias("opened_at_dense_rank"),
        F.last("requester_id", ignorenulls=True).over(w_agent_id_priority_by_opened_3).alias("last_known_requester_id"),
        F.max("requester_id").over(w_priority_agent_team).alias("priority_agent_team_max_requester_id"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_agent_id_priority_by_opened_4))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.last("requester_id").over(w_agent_team_agent_id_by_opene).alias("last_requester_id"),
        F.sum("requester_id").over(w_priority_agent_team).alias("priority_agent_team_sum_requester_id"),
        F.first("requester_id").over(w_agent_team_agent_id_by_opene).alias("first_requester_id_2"),
    )
)

enriched = (
    ranked
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "agent_name",
        "agent_team",
        "requester_id_avg_7",
        "opened_at_dense_rank",
        "last_known_requester_id",
        "priority_agent_team_max_requester_id",
        "last_requester_id",
        "priority_agent_team_sum_requester_id",
        "first_requester_id_2",
        F.lower(F.regexp_replace(F.trim(F.col("agent_team")), " ", "_")).alias("agent_team_slug"),
    )
)

result = (
    enriched
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "agent_name",
        "requester_id_avg_7",
        "last_known_requester_id",
        "priority_agent_team_max_requester_id",
        "last_requester_id",
        "priority_agent_team_sum_requester_id",
        "first_requester_id_2",
    )
    .orderBy(
        F.col("last_requester_id").desc(),
        F.col("opened_at").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
