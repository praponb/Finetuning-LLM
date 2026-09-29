from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_requester_id = Window.partitionBy("channel").orderBy("requester_id", "id")
w_channel_by_requester_id_full = Window.partitionBy("channel").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "upgrade")) | (F.col("agent_id").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        F.concat(F.col("priority"), F.lit("#"), F.col("id").cast("string")).alias("priority_ref"),
        (F.col("opened_at") + F.expr("INTERVAL 7 DAYS")).alias("opened_at_plus_7d"),
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.round(F.percent_rank().over(w_channel_by_requester_id), 3).alias("requester_id_percent_rank"),
        F.nth_value("requester_id", 2).over(w_channel_by_requester_id_full).alias("second_requester_id"),
        F.round((F.unix_timestamp("opened_at_plus_7d") - F.unix_timestamp(F.lag("opened_at_plus_7d").over(w_channel_by_requester_id))) / 60, 1).alias("minutes_since_prev_opened_at_plus_7d"),
        F.nth_value("opened_at_plus_7d", 2).over(w_channel_by_requester_id_full).alias("second_opened_at_plus_7d"),
    )
)

with_activity = (
    with_windows.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team") != "Onboarding"), "left_semi")
)

result = (
    with_activity
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "priority_ref",
        "opened_at_plus_7d",
        "channel_prefix",
        "requester_id_percent_rank",
        "second_requester_id",
        "minutes_since_prev_opened_at_plus_7d",
        F.substring(F.trim(F.col("priority_ref")), 1, 2).alias("priority_ref_prefix"),
        F.month(F.col("second_opened_at_plus_7d")).alias("second_opened_at_plus_7d_month"),
    )
    .orderBy(F.col("priority").desc(), F.col("priority_ref").desc())
)
