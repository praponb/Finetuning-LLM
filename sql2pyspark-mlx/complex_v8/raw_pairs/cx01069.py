from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_closed_at_plus_15m = Window.partitionBy("agent_id", F.month("closed_at_plus_15m")).orderBy("closed_at")
w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_closed_at_to_date_by_minutes = Window.partitionBy(F.to_date("closed_at")).orderBy(F.col("minutes_closed_at_to_opened_at").desc(), "id").rowsBetween(-2, -1)
w_agent_id_closed_at_plus_15m_2 = Window.partitionBy("agent_id", F.month("closed_at_plus_15m")).orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_closed_at_plus_15m_3 = Window.partitionBy("agent_id", F.month("closed_at_plus_15m")).orderBy("closed_at", "id")

prepared = (
    df_tickets
    .filter(F.col("channel") != "email")
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 60, 1).alias("minutes_closed_at_to_opened_at"),
        (F.col("closed_at") + F.expr("INTERVAL 15 MINUTES")).alias("closed_at_plus_15m"),
        F.date_format(F.col("opened_at"), "MMM yyyy").alias("opened_at_label"),
    )
)

without_matches = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.requester_id") <= 18) & (F.col("t.closed_at") < F.lit("2025-02-20 15:00:00").cast("timestamp")), "left_semi")
)

ranked = (
    without_matches
    .select(
        "*",
        F.round(F.cume_dist().over(w_agent_id_closed_at_plus_15m), 3).alias("closed_at_cume_dist"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id_csat), 2).alias("csat_vs_agent_id_csat_avg"),
        F.max("csat").over(w_closed_at_to_date_by_minutes).alias("csat_max_2_prior"),
        F.first("csat").over(w_agent_id_closed_at_plus_15m_2).alias("first_csat"),
        F.ntile(5).over(w_agent_id_closed_at_plus_15m_3).alias("closed_at_quintile"),
    )
)

result = (
    ranked
    .cube("csat", "agent_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.min("closed_at_cume_dist").alias("min_closed_at_cume_dist"),
        F.count("*").alias("cnt"),
    )
)
