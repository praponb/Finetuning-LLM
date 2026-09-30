from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_stddev_csat = Window.partitionBy("agent_id").orderBy(F.col("stddev_csat").desc(), "priority", "channel", "agent_id")
w_priority = Window.partitionBy("priority")

base = (
    df_tickets
    .filter((F.col("agent_id") <= 7) & (F.col("requester_id") > 11))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
    )
)

without_matches = (
    base.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type") == "replied") & (F.hour(F.col("t.event_at")).between(6, 10)), "left_semi")
)

summary = (
    without_matches
    .groupBy("priority", "channel", "agent_id")
    .agg(
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.round(F.stddev("csat"), 2).alias("stddev_csat"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 3)
)

with_windows = (
    summary
    .select(
        "priority",
        "channel",
        "agent_id",
        "channel_list",
        "stddev_csat",
        F.round(F.percent_rank().over(w_agent_id_by_stddev_csat), 3).alias("stddev_csat_percent_rank"),
        F.round(F.col("stddev_csat") / F.nullif(F.sum("stddev_csat").over(w_priority), F.lit(0)), 4).alias("stddev_csat_share_of_priority"),
        F.ntile(3).over(w_agent_id_by_stddev_csat).alias("stddev_csat_tercile"),
    )
)

joined = (
    with_windows.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.agent_name").isNotNull()), "left")
    .select(
        "s.priority",
        "s.channel",
        "s.agent_id",
        "s.channel_list",
        "s.stddev_csat_share_of_priority",
        "s.stddev_csat_tercile",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    joined
    .select(
        "priority",
        "agent_id",
        "channel_list",
        "stddev_csat_share_of_priority",
        "stddev_csat_tercile",
        "agent_hired_on",
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
    )
    .orderBy(F.col("agent_id").asc())
)
