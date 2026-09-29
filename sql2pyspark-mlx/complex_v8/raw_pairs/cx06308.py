from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id", "priority").orderBy(F.col("least_csat_requester_id").desc(), F.col("id").desc())

recent = (
    df_tickets
    .filter(~F.col("priority").isin("P2", "P1"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        F.date_trunc("week", F.col("opened_at")).alias("opened_at_week"),
        F.least(F.col("csat"), F.col("requester_id")).alias("least_csat_requester_id"),
    )
)

unique_rows = (
    recent
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_details_agent = (
    unique_rows.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.team") != "Billing")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.least_csat_requester_id",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    with_details_agent
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "least_csat_requester_id",
        (F.floor(F.col("least_csat_requester_id") / 5) * 5).alias("least_csat_requester_id_bucket"),
    )
    .orderBy("closed_at", F.col("least_csat_requester_id_bucket").asc_nulls_last())
)
