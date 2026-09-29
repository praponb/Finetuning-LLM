from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("priority", "agent_id").orderBy(F.col("csat").desc(), F.col("id").desc())
w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_csat_closed_at_to_date_by_cs = Window.partitionBy("csat", F.to_date("closed_at")).orderBy(F.col("csat").desc(), "id").rowsBetween(-3, -1)

cleaned = (
    df_tickets
    .filter(F.col("csat") >= 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

windowed = (
    latest
    .select(
        "*",
        F.min("csat").over(w_agent_id_csat).alias("agent_id_csat_min_csat"),
        F.round(F.avg("requester_id").over(w_csat_closed_at_to_date_by_cs), 2).alias("requester_id_avg_3_prior"),
    )
)

latest_2 = (
    windowed
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched_agent = (
    latest_2.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on") >= F.lit("2024-07-09").cast("date")), "inner")
    .select(
        "s.*",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

result = (
    enriched_agent
    .filter(F.col("agent_id_csat_min_csat") >= 21)
    .select("id", "agent_name", "priority")
    .distinct()
)
