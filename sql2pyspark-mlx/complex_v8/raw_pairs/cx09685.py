from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_team").orderBy("minutes_opened_at_to_closed_at", "id")

recent = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "refund")) | (F.col("requester_id") <= 18))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
        F.lower(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

with_parent_agent = (
    recent.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.lower(F.col("su.agent_name")).like("%na%")), "left")
    .select("s.*", F.col("su.team").alias("agent_team"))
)

agent_team_stats = (
    with_parent_agent
    .groupBy("agent_team")
    .agg(F.avg("requester_id_bucket").alias("avg_requester_id_bucket"))
)

compared = (
    with_parent_agent.alias("s")
    .join(agent_team_stats.alias("g"), F.col("g.agent_team") == F.col("s.agent_team"), "inner")
    .filter(F.col("s.requester_id_bucket") > F.col("g.avg_requester_id_bucket"))
    .select(
        "s.*",
        "g.avg_requester_id_bucket",
        F.round(F.col("s.requester_id_bucket") - F.col("g.avg_requester_id_bucket"), 2).alias("requester_id_bucket_minus_avg"),
    )
)

derived = (
    compared
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        "requester_id_bucket",
        "priority_clean",
        "agent_team",
        "avg_requester_id_bucket",
        "requester_id_bucket_minus_avg",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
        (F.col("opened_at") + F.expr("INTERVAL 1 DAYS")).alias("opened_at_plus_1d"),
    )
)

deduped = (
    derived
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        "requester_id_bucket",
        "priority_clean",
        "agent_team",
        "avg_requester_id_bucket",
        "requester_id_bucket_minus_avg",
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
        F.length(F.trim(F.col("priority_clean"))).alias("priority_clean_len"),
    )
    .orderBy(F.col("opened_at").asc_nulls_last())
)
