from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_team", "channel").orderBy(F.col("closed_at").desc(), F.col("id").desc())

cleaned = (
    df_tickets
    .filter((F.col("channel") == "web") | (F.col("csat") > 1))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "tags",
    )
)

with_details = (
    cleaned.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

latest_per_key = (
    with_details
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

filtered = (
    latest_per_key.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.opened_at") >= F.lit("2025-01-13 11:00:00").cast("timestamp")), "left_anti")
)

result = (
    filtered
    .filter(F.col("agent_hired_on").isNotNull())
    .select(
        "id",
        "agent_id",
        "channel",
        "csat",
        "agent_hired_on",
        "agent_team",
    )
    .orderBy(F.col("agent_team").desc(), F.col("channel").desc())
)
