from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id").orderBy(F.col("channel_len").desc(), F.col("id").desc())

cleaned_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

deduped = (
    cleaned_tickets
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined_agent = (
    deduped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", F.col("su.team").alias("agent_team"), "su.agent_name")
)

result = (
    joined_agent
    .groupBy("opened_at")
    .pivot("priority", ["P2", "P4"])
    .agg(F.max("csat"))
)
