from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_or_unknown_channel = Window.partitionBy("priority_or_unknown", "channel").orderBy("opened_at", "id")
w_priority_or_unknown_agent_id = Window.partitionBy("priority_or_unknown", "agent_id").orderBy("opened_at")
w_priority_or_unknown_agent_id_2 = Window.partitionBy("priority_or_unknown", "agent_id").orderBy(F.col("opened_at").desc(), "id")

cleaned_tickets = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 2) & (F.col("agent_id").isNotNull()))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug_2"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

scored = (
    cleaned_tickets
    .withColumn("channel_slug_channel_slug_2_label", F.concat_ws("-", F.col("channel_slug"), F.col("channel_slug_2")))
)

metrics = (
    scored
    .select(
        "*",
        F.round(F.percent_rank().over(w_priority_or_unknown_channel), 3).alias("opened_at_percent_rank"),
        F.dense_rank().over(w_priority_or_unknown_agent_id).alias("opened_at_dense_rank"),
        F.lag("closed_at").over(w_priority_or_unknown_agent_id_2).alias("prev_closed_at"),
    )
)

per_label = (
    metrics
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "channel_slug",
        "channel_slug_2",
        "priority_or_unknown",
        "channel_slug_channel_slug_2_label",
        "opened_at_percent_rank",
        "opened_at_dense_rank",
        "prev_closed_at",
        F.posexplode("tags").alias("tag_pos", "tag"),
    )
)

derived = per_label.withColumn("closed_at_label", F.date_format(F.col("closed_at"), "MMM yyyy"))

result = (
    derived
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel_slug",
        "channel_slug_2",
        "channel_slug_channel_slug_2_label",
        "opened_at_percent_rank",
        "opened_at_dense_rank",
        "tag_pos",
        "tag",
        F.to_date(F.col("prev_closed_at")).alias("prev_closed_at_date"),
    )
    .orderBy(
        F.col("opened_at").desc(),
        F.col("channel_slug_2").desc_nulls_first(),
        "id",
        "tag_pos",
    )
    .limit(20)
)
