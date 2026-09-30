from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_priority_agent_id_by_request = Window.partitionBy("priority", "agent_id").orderBy("requester_id", "id").rowsBetween(-3, -1)
w_agent_hired_on_month = Window.partitionBy(F.date_trunc("month", "agent_hired_on"))
w_agent_id_opened_at_month_by = Window.partitionBy("agent_id", F.date_trunc("month", "opened_at")).orderBy("closed_at_day", "id").rowsBetween(-2, Window.currentRow)
w_priority_by_agent_hired_on_p = Window.partitionBy("priority").orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(-3, -1)
w_agent_id_by_requester_id_tie = Window.partitionBy("agent_id").orderBy(F.col("requester_id").desc())
w_agent_id_by_requester_id = Window.partitionBy("agent_id").orderBy(F.col("requester_id").desc(), "id")
w_closed_at_day_month_opened_a = Window.partitionBy(F.date_trunc("month", "closed_at_day"), F.date_trunc("month", "opened_at")).orderBy("closed_at_day", "id").rowsBetween(-2, -1)
w_priority_agent_id_by_request_2 = Window.partitionBy("priority", "agent_id").orderBy("requester_id")

cleaned_tickets = (
    df_tickets
    .filter(F.month(F.col("closed_at")).isin(1, 3))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.date_trunc("day", F.col("closed_at")).alias("closed_at_day"),
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
    )
)

enriched_agent = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.tags",
        "s.closed_at_day",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

windowed = (
    enriched_agent
    .select(
        "*",
        F.round(F.avg("requester_id").over(w_agent_id), 2).alias("agent_id_avg_requester_id"),
        F.max("requester_id").over(w_priority_agent_id_by_request).alias("requester_id_max_3_prior"),
        F.count(F.lit(1)).over(w_agent_hired_on_month).alias("rows_in_agent_hired_on_month"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_hired_on_month), F.lit(0)), 4).alias("requester_id_share_of_agent_hired_on_month"),
        F.size(F.collect_set("agent_team").over(w_agent_id)).alias("distinct_agent_team_in_agent_id"),
        F.size(F.collect_set("priority").over(w_agent_id)).alias("distinct_priority_in_agent_id"),
        F.round(F.avg("requester_id").over(w_agent_id_opened_at_month_by), 2).alias("requester_id_avg_3"),
        F.max("requester_id").over(w_priority_by_agent_hired_on_p).alias("requester_id_max_3_prior_2"),
        F.rank().over(w_agent_id_by_requester_id_tie).alias("requester_id_rank"),
        F.dense_rank().over(w_agent_id_by_requester_id_tie).alias("requester_id_dense_rank"),
        F.lag("closed_at_day", 2).over(w_agent_id_by_requester_id).alias("prev2_closed_at_day"),
        F.round(F.avg("requester_id").over(w_closed_at_day_month_opened_a), 2).alias("requester_id_avg_2_prior"),
        F.rank().over(w_priority_agent_id_by_request_2).alias("requester_id_rank_2"),
    )
)

enriched = (
    windowed
    .select(
        "id",
        "agent_id",
        "opened_at",
        "tags",
        "closed_at_day",
        "agent_name",
        "requester_id_max_3_prior",
        "rows_in_agent_hired_on_month",
        "requester_id_share_of_agent_hired_on_month",
        "distinct_agent_team_in_agent_id",
        "requester_id_avg_3",
        "requester_id_max_3_prior_2",
        "requester_id_rank",
        "requester_id_dense_rank",
        "prev2_closed_at_day",
        "requester_id_avg_2_prior",
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
    )
)

result = (
    enriched
    .filter(F.col("requester_id_max_3_prior") < 56)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "tags",
        "closed_at_day",
        "agent_name",
        "requester_id_max_3_prior",
        "requester_id_share_of_agent_hired_on_month",
        "distinct_agent_team_in_agent_id",
        "requester_id_avg_3",
        "requester_id_max_3_prior_2",
        "requester_id_rank",
        "requester_id_dense_rank",
        "prev2_closed_at_day",
        "requester_id_avg_2_prior",
        "tags_text",
    )
)
