from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("closed_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_tickets
    .filter(F.col("priority") == "P4")
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

with_prev = filtered.withColumn("prev_closed_at", F.lag("closed_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_channel_running))

enriched = (
    sessionized.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team").isNotNull()), "left")
    .select("s.*", F.col("su.team").alias("agent_team"), "su.agent_name")
)

result = (
    enriched
    .select(
        "id",
        "closed_at",
        "priority",
        "channel",
        "channel_slug",
        "prev_closed_at",
        "is_new_session",
        "agent_team",
        F.date_trunc("month", F.col("closed_at")).alias("closed_at_month"),
    )
    .orderBy(F.col("agent_team").asc(), "channel")
)
