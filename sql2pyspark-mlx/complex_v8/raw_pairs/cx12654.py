from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), F.col("id").desc())

prepared = (
    df_tickets
    .filter(~F.col("channel").isin("phone", "chat"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.date_trunc("month", F.col("opened_at")).alias("opened_at_month"),
    )
)

latest_per_key = (
    prepared
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_details_agent = (
    latest_per_key.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.hired_on").between(F.lit("2024-05-02").cast("date"), F.lit("2025-03-30").cast("date")))
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.tags",
        "s.opened_at_month",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

annotated = (
    with_details_agent
    .withColumn("agent_name_ref", F.concat(F.col("agent_name"), F.lit("#"), F.col("csat").cast("string")))
)

agent_id_stats = (
    annotated
    .groupBy("agent_id")
    .agg(F.avg("csat").alias("avg_csat"))
)

outliers = (
    annotated.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.csat") > F.col("g.avg_csat"))
    .select(
        "s.*",
        "g.avg_csat",
        F.round(F.col("s.csat") - F.col("g.avg_csat"), 2).alias("csat_minus_avg"),
    )
)

result = (
    outliers
    .groupBy("agent_hired_on")
    .pivot("priority", ["P4", "P2"])
    .agg(F.sum("csat_minus_avg").alias("amount"), F.max("csat_minus_avg").alias("max_v"))
    .orderBy("agent_hired_on")
)
