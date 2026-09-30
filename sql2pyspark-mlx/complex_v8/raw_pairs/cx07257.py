from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("tag").orderBy("tag_pos", "id", "tag_pos")
w_tag_by_opened_at = Window.partitionBy("tag").orderBy(F.col("opened_at").asc_nulls_last(), "id", "tag_pos")
w_channel = Window.partitionBy("channel")
w_agent_id_by_opened_at = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), "id", "tag_pos")
w_channel_by_greatest_requeste = Window.partitionBy("channel").orderBy("greatest_requester_id_csat", "id", "tag_pos").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_tickets
    .filter((F.hour(F.col("closed_at")).between(6, 12)) & (F.col("requester_id") < 23))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
    )
)

per_label = (
    recent
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "greatest_requester_id_csat",
        F.posexplode_outer("tags").alias("tag_pos", "tag"),
    )
)

deduped = (
    per_label
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

ranked = (
    deduped
    .select(
        "id",
        "agent_id",
        "opened_at",
        "tag_pos",
        "tag",
        F.ntile(5).over(w_tag_by_opened_at).alias("opened_at_quintile"),
        F.size(F.collect_set("agent_id").over(w_channel)).alias("distinct_agent_id_in_channel"),
        F.ntile(5).over(w_agent_id_by_opened_at).alias("opened_at_quintile_2"),
        F.last("greatest_requester_id_csat").over(w_channel_by_greatest_requeste).alias("last_greatest_requester_id_csat"),
    )
)

result = (
    ranked
    .select(
        "id",
        "agent_id",
        "opened_at",
        "tag_pos",
        "tag",
        "opened_at_quintile_2",
    )
    .orderBy(F.col("agent_id").desc_nulls_first())
)
