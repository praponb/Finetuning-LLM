from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_csat_moving_6 = Window.orderBy("csat", "id").rowsBetween(-6, Window.currentRow)
w_all_by_csat_full = Window.orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_closed_at_to_date = Window.partitionBy("csat", F.to_date("closed_at"))

valid = (
    df_tickets
    .filter((F.col("agent_id") <= 5) | (F.hour(F.col("opened_at")).between(7, 12)))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
    )
)

enriched = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        F.col("su.team").alias("agent_team"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.round(F.avg("csat").over(w_all_by_csat_moving_6), 2).alias("csat_avg_7"),
        F.last("closed_at").over(w_all_by_csat_full).alias("last_closed_at"),
        F.round(F.avg("csat").over(w_csat_closed_at_to_date), 2).alias("csat_closed_at_to_date_avg_csat"),
        F.min("csat").over(w_csat_closed_at_to_date).alias("csat_closed_at_to_date_min_csat"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "agent_id",
        "closed_at",
        "csat",
        "agent_team",
        "csat_avg_7",
        "csat_closed_at_to_date_avg_csat",
        "csat_closed_at_to_date_min_csat",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
    )
)
