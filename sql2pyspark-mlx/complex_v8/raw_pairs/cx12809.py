from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_by_requester_id_ful = Window.partitionBy("priority").orderBy(F.col("requester_id").asc_nulls_last(), "id", "tag").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_tag = Window.partitionBy("agent_id", "tag")
w_agent_id_csat_by_requester_i = Window.partitionBy("agent_id", "csat").orderBy(F.col("requester_id").desc())
w_csat_by_csat_ties = Window.partitionBy("csat").orderBy("csat")

scoped_tickets = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.col("csat") <= 5))
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
        F.concat_ws("-", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

with_parent_agent = (
    scoped_tickets.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.length(F.trim(F.col("su.agent_name"))) >= 8), "inner")
    .filter(F.length(F.trim(F.col("su.agent_name"))) >= 7)
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.channel_priority_label",
        "su.agent_name",
    )
)

annotated = (
    with_parent_agent
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        "channel_priority_label",
        F.upper(F.trim(F.col("agent_name"))).alias("agent_name_clean"),
    )
)

with_activity = (
    annotated.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.csat").between(2, 5)), "left_anti")
)

exploded = (
    with_activity
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "channel_priority_label",
        "agent_name_clean",
        F.explode_outer("tags").alias("tag"),
    )
)

sequenced = (
    exploded
    .select(
        "id",
        "requester_id",
        "csat",
        "agent_name_clean",
        "tag",
        F.first("csat").over(w_priority_by_requester_id_ful).alias("first_csat"),
        F.min("csat").over(w_agent_id_tag).alias("agent_id_tag_min_csat"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id_tag), F.lit(0)), 4).alias("requester_id_share_of_agent_id_tag"),
        F.round(F.percent_rank().over(w_agent_id_csat_by_requester_i), 3).alias("requester_id_percent_rank"),
        F.dense_rank().over(w_csat_by_csat_ties).alias("csat_dense_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "requester_id",
        "csat",
        "agent_name_clean",
        "tag",
        "agent_id_tag_min_csat",
        "requester_id_percent_rank",
        "csat_dense_rank",
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
        F.coalesce(F.col("first_csat"), F.lit(0)).alias("first_csat_filled"),
    )
    .orderBy(F.col("first_csat_filled").desc())
)
