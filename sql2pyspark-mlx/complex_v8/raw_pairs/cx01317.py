from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_year = Window.partitionBy(F.year("closed_at"))
w_channel_by_agent_hired_on = Window.partitionBy("channel").orderBy(F.col("agent_hired_on").asc_nulls_last(), "id")
w_csat_priority = Window.partitionBy("csat", "priority")
w_agent_id_by_csat = Window.partitionBy("agent_id").orderBy("csat", "id")
w_channel_by_agent_hired_on_fu = Window.partitionBy("channel").orderBy(F.col("agent_hired_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_priority_by_opened_at = Window.partitionBy("csat", "priority").orderBy(F.col("opened_at").desc(), "id")
w_csat_priority_by_opened_at_r = Window.partitionBy("csat", "priority").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_csat_running = Window.partitionBy("agent_id").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_csat_ties = Window.partitionBy("agent_id").orderBy("csat")
w_priority = Window.partitionBy("priority")
w_all_by_csat_share_of_closed = Window.orderBy(F.col("csat_share_of_closed_at_year").desc())
w_all_by_csat_share_of_closed_2 = Window.orderBy(F.col("csat_share_of_closed_at_year").desc(), "id")

cleaned = (
    df_tickets
    .filter((F.col("csat") <= 4) & (F.array_contains(F.col("tags"), "bug")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.substring(F.trim(F.col("priority")), 1, 4).alias("priority_prefix"),
    )
)

with_details = (
    cleaned.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.hired_on") < F.lit("2025-02-26").cast("date"))
    .select("s.*", F.col("su.hired_on").alias("agent_hired_on"))
)

with_trends = (
    with_details
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "agent_hired_on",
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_closed_at_year), F.lit(0)), 4).alias("csat_share_of_closed_at_year"),
        F.ntile(4).over(w_channel_by_agent_hired_on).alias("agent_hired_on_quartile"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat_priority), F.lit(0)), 4).alias("csat_share_of_csat_priority"),
        F.lag("agent_hired_on").over(w_agent_id_by_csat).alias("prev_agent_hired_on"),
        F.nth_value("agent_hired_on", 2).over(w_channel_by_agent_hired_on_fu).alias("second_agent_hired_on"),
        F.lag("agent_hired_on").over(w_csat_priority_by_opened_at).alias("prev_agent_hired_on_2"),
        F.max("csat").over(w_csat_priority_by_opened_at_r).alias("max_csat_so_far"),
        F.min("csat").over(w_agent_id_by_csat_running).alias("min_csat_so_far"),
        F.round(F.avg("csat").over(w_csat_priority), 2).alias("csat_priority_avg_csat"),
        F.min("csat").over(w_closed_at_year).alias("closed_at_year_min_csat"),
        F.round(F.col("csat") - F.avg("csat").over(w_closed_at_year), 2).alias("csat_vs_closed_at_year_avg"),
        F.dense_rank().over(w_agent_id_by_csat_ties).alias("csat_dense_rank"),
    )
)

top_rows = with_trends.filter(F.col("csat_dense_rank") == 1)

windowed = (
    top_rows
    .select(
        "id",
        "closed_at",
        "priority",
        "agent_hired_on",
        "csat_share_of_closed_at_year",
        "agent_hired_on_quartile",
        "csat_share_of_csat_priority",
        "second_agent_hired_on",
        "prev_agent_hired_on_2",
        "max_csat_so_far",
        "min_csat_so_far",
        "closed_at_year_min_csat",
        "csat_vs_closed_at_year_avg",
        F.max("csat_vs_closed_at_year_avg").over(w_priority).alias("priority_max_csat_vs_closed_at_year_avg"),
        F.round(F.col("agent_hired_on_quartile") - F.avg("agent_hired_on_quartile").over(w_priority), 2).alias("agent_hired_on_quartile_vs_priority_avg"),
        F.dense_rank().over(w_all_by_csat_share_of_closed).alias("csat_share_of_closed_at_year_dense_rank"),
        F.dense_rank().over(w_all_by_csat_share_of_closed_2).alias("csat_share_of_closed_at_year_dense_rank_2"),
    )
)

result = (
    windowed
    .select(
        "id",
        "agent_hired_on",
        "csat_share_of_closed_at_year",
        "agent_hired_on_quartile",
        "second_agent_hired_on",
        "prev_agent_hired_on_2",
        "max_csat_so_far",
        "min_csat_so_far",
        "csat_vs_closed_at_year_avg",
        "priority_max_csat_vs_closed_at_year_avg",
        "csat_share_of_closed_at_year_dense_rank",
        "csat_share_of_closed_at_year_dense_rank_2",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("second_agent_hired_on")).alias("days_since_second_agent_hired_on"),
    )
    .orderBy(
        F.col("csat_share_of_closed_at_year_dense_rank").asc_nulls_last(),
        F.col("agent_hired_on_quartile").desc(),
    )
)
