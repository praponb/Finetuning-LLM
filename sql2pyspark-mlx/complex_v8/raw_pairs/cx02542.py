from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_month_by_least_csa = Window.partitionBy(F.month("opened_at")).orderBy("least_csat_requester_id", "id").rowsBetween(-3, -1)
w_opened_at_to_date_agent_id = Window.partitionBy(F.to_date("opened_at"), "agent_id")
w_priority_channel_by_csat = Window.partitionBy("priority", "channel").orderBy("csat", "id")

scoped = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.least(F.col("csat"), F.col("requester_id")).alias("least_csat_requester_id"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.sum("least_csat_requester_id").over(w_opened_at_month_by_least_csa).alias("least_csat_requester_id_sum_3_prior"),
        F.size(F.collect_set("priority").over(w_opened_at_to_date_agent_id)).alias("distinct_priority_in_opened_at_to_date_agent_id"),
        F.count(F.lit(1)).over(w_opened_at_to_date_agent_id).alias("rows_in_opened_at_to_date_agent_id"),
        F.lag("closed_at", 2).over(w_priority_channel_by_csat).alias("prev2_closed_at"),
    )
)

screened = (
    metrics.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isNotNull()), "left_anti")
)

result = (
    screened
    .select(
        "id",
        F.col("csat").alias("m_csat"),
        F.col("distinct_priority_in_opened_at_to_date_agent_id").alias("m_distinct_priority_in_opened_at_to_date_agent_i"),
    )
    .unpivot(
        ids=["id"],
        values=["m_csat", "m_distinct_priority_in_opened_at_to_date_agent_i"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
)
