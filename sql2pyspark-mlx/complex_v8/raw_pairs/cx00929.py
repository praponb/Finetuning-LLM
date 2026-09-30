from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("priority"), F.lit("#"), F.col("csat").cast("string")).alias("priority_ref"),
    )
)

with_prev = recent.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_agent_id_running))

enriched = (
    sessionized
    .filter(~F.array_contains(F.col("tags"), "login"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        "priority_ref",
        "is_new_session",
        (F.col("burst_id") + F.col("is_new_session")).alias("burst_id_plus_is_new_session"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)

result = (
    enriched
    .filter(F.col("burst_id_plus_is_new_session") < 5)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        "priority_ref",
        "burst_id_plus_is_new_session",
        "channel_or_unknown",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
        F.date_trunc("quarter", F.col("opened_at")).alias("opened_at_quarter"),
    )
    .orderBy(F.col("agent_id").asc_nulls_last(), "opened_at", "id")
    .limit(10)
)
