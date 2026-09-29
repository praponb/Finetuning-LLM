from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("closed_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_prev = filtered.withColumn("prev_closed_at", F.lag("closed_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_closed_at").isNull() | ((F.unix_timestamp("closed_at") - F.unix_timestamp("prev_closed_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_channel_running))

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.min("requester_id").alias("min_ticket_requester_id"))
)

summarized_rows = (
    sessionized.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.min_ticket_requester_id")
)

derived = (
    summarized_rows
    .select(
        "id",
        "requester_id",
        "closed_at",
        "csat",
        "tags",
        "session_id",
        "min_ticket_requester_id",
        F.when(F.col("priority").isin("P4", "P2"), "group_a").when(F.col("priority") == "P3", "group_b").otherwise("other").alias("priority_group"),
        F.quarter(F.col("prev_closed_at")).alias("prev_closed_at_quarter"),
        F.when(F.col("channel").isin("email", "phone"), "group_a").when(F.col("channel") == "web", "group_b").otherwise("other").alias("channel_group"),
    )
)

result = (
    derived
    .groupingSets(
        [["prev_closed_at_quarter", "priority_group", "session_id"], ["prev_closed_at_quarter"]],
        "prev_closed_at_quarter", "priority_group", "session_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.sum("prev_closed_at_quarter") / F.nullif(F.sum("csat"), F.lit(0)), 3).alias("prev_closed_at_quarter_to_csat_ratio"),
        F.countDistinct("priority_group").alias("distinct_priority_groups"),
        F.sum("requester_id").alias("total_requester_id"),
    )
    .orderBy(
        F.col("prev_closed_at_quarter").asc_nulls_last(),
        F.col("priority_group").asc_nulls_last(),
        F.col("session_id").asc_nulls_last(),
    )
)
