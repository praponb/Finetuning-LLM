from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("channel", "agent_id").orderBy(F.col("channel_len").desc(), F.col("id").desc())

filtered_tickets = (
    df_tickets
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
        F.date_trunc("hour", F.col("closed_at")).alias("closed_at_hour"),
        F.size(F.col("tags")).alias("tags_count"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

filtered = (
    filtered_tickets.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.csat") <= 4) & (F.hour(F.col("t.opened_at")).between(7, 11)), "left_semi")
    .filter(F.size(F.col("tags")) >= 2)
)

labeled = (
    filtered
    .filter(F.col("agent_id") >= 8)
    .select(
        "*",
        F.abs(F.col("tags_count") - 2).alias("tags_count_distance"),
        F.round(F.col("requester_id") / F.nullif(F.col("tags_count"), F.lit(0)), 3).alias("requester_id_per_tags_count"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

latest_per_key = (
    labeled
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .groupBy("closed_at", "agent_id")
    .pivot("channel", ["chat", "phone"])
    .agg(F.max("channel_len"))
)
