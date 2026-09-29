from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_agent_id = Window.partitionBy("csat", "agent_id")
w_csat_by_csat = Window.partitionBy("csat").orderBy(F.col("csat").desc(), "id")

recent = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.col("channel") != "chat"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
    )
)

sequenced = (
    recent
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "tags",
        F.count(F.lit(1)).over(w_csat_agent_id).alias("rows_in_csat_agent_id"),
        F.size(F.collect_set("channel").over(w_csat_agent_id)).alias("distinct_channel_in_csat_agent_id"),
        F.dense_rank().over(w_csat_by_csat).alias("csat_dense_rank"),
    )
)

combined_details_agent = (
    sequenced.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.tags",
        "s.rows_in_csat_agent_id",
        "s.distinct_channel_in_csat_agent_id",
        "su.agent_name",
    )
)

enriched = (
    combined_details_agent.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on") >= F.lit("2024-05-02").cast("date")), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.rows_in_csat_agent_id",
        "s.distinct_channel_in_csat_agent_id",
        "s.agent_name",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.agent_name").alias("agent_agent_name"),
    )
)

annotated = (
    enriched
    .select(
        "*",
        (F.col("opened_at") + F.expr("INTERVAL 30 MINUTES")).alias("opened_at_plus_30m"),
        F.initcap(F.trim(F.col("agent_tier"))).alias("agent_tier_clean"),
    )
)

grouped = (
    annotated
    .filter(F.col("rows_in_csat_agent_id") >= 4)
    .groupBy(F.date_format("opened_at", "yyyy-MM").alias("opened_at_ym"))
    .agg(
        F.sum(F.when(F.col("agent_agent_name").isNotNull(), 1).otherwise(0)).alias("n_agent_agent_name_is_not_null"),
        F.max("opened_at").alias("last_opened_at"),
    )
)

tagged = (
    grouped
    .filter(F.col("last_opened_at") >= F.lit("2025-01-19 07:00:00").cast("timestamp"))
    .withColumn("opened_at_ym_prefix", F.substring(F.trim(F.col("opened_at_ym")), 1, 4))
)

result = tagged.select("opened_at_ym", "last_opened_at", "opened_at_ym_prefix")
