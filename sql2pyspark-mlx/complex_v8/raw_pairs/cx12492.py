from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_requester_id_buck = Window.partitionBy("channel").orderBy("requester_id_bucket", "id")
w_priority_agent_id_by_request = Window.partitionBy("priority", "agent_id").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 2) & (F.col("agent_id").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        (F.col("csat") - F.col("requester_id")).alias("csat_minus_requester_id"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        (F.col("csat_minus_requester_id") - F.lag("csat_minus_requester_id").over(w_channel_by_requester_id_buck)).alias("csat_minus_requester_id_change"),
        F.nth_value("requester_id", 2).over(w_priority_agent_id_by_request).alias("second_requester_id"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

profiled = (
    with_trends.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

tagged = (
    profiled
    .select(
        "*",
        F.initcap(F.trim(F.col("channel"))).alias("channel_clean"),
        F.round(F.col("requester_id") / F.nullif(F.col("requester_id_bucket"), F.lit(0)), 3).alias("requester_id_per_requester_id_bucket"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

result = (
    tagged
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "requester_id_bucket",
        "priority_slug",
        "csat_minus_requester_id_change",
        "second_requester_id",
        "ticket_count",
        "channel_len",
        F.round(F.col("requester_id_bucket") / F.nullif(F.col("channel_len"), F.lit(0)), 3).alias("requester_id_bucket_per_channel_len"),
        F.concat(F.col("channel"), F.lit("#"), F.col("ticket_count").cast("string")).alias("channel_ref"),
    )
    .orderBy(F.col("csat").asc(), "channel_ref")
)
