from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_closed_at_to_date = Window.partitionBy("channel", F.to_date("closed_at"))
w_agent_id_closed_at_year_by_d = Window.partitionBy("agent_id", F.year("closed_at")).orderBy("days_since_opened_at", "id")
w_all_by_closed_at_full = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_closed_at_prior_1 = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(-1, -1)

base = (
    df_tickets
    .filter((F.col("csat") < 4) & (F.col("priority") == "P2"))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        F.date_trunc("hour", F.col("closed_at")).alias("closed_at_hour"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

sequenced = (
    base
    .select(
        "*",
        F.min("days_since_opened_at").over(w_channel_closed_at_to_date).alias("channel_closed_at_to_date_min_days_since_opened_"),
        F.lag("closed_at", 2).over(w_agent_id_closed_at_year_by_d).alias("prev2_closed_at"),
        F.max("days_since_opened_at").over(w_channel_closed_at_to_date).alias("channel_closed_at_to_date_max_days_since_opened_"),
        F.last("closed_at_hour").over(w_all_by_closed_at_full).alias("last_closed_at_hour"),
        F.round(F.col("days_since_opened_at") / F.nullif(F.sum("days_since_opened_at").over(w_channel_closed_at_to_date), F.lit(0)), 4).alias("days_since_opened_at_share_of_channel_closed_at_"),
        F.size(F.collect_set("agent_id").over(w_channel_closed_at_to_date)).alias("distinct_agent_id_in_channel_closed_at_to_date"),
        F.sum("days_since_opened_at").over(w_all_by_closed_at_prior_1).alias("days_since_opened_at_sum_1_prior"),
        F.round(F.col("days_since_opened_at") - F.avg("days_since_opened_at").over(w_channel_closed_at_to_date), 2).alias("days_since_opened_at_vs_channel_closed_at_to_dat"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "days_since_opened_at",
        "channel_closed_at_to_date_max_days_since_opened_",
        "last_closed_at_hour",
        "days_since_opened_at_sum_1_prior",
        "days_since_opened_at_vs_channel_closed_at_to_dat",
        F.dayofweek(F.col("closed_at")).alias("closed_at_dayofweek"),
        F.coalesce(F.col("days_since_opened_at"), F.lit(0)).alias("days_since_opened_at_filled"),
    )
    .orderBy(
        F.col("agent_id").desc(),
        "days_since_opened_at_vs_channel_closed_at_to_dat",
        "id",
    )
    .limit(20)
)
