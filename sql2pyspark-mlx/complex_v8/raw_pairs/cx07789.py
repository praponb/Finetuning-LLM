from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat = Window.partitionBy("csat")
w_priority_closed_at_month_by = Window.partitionBy("priority", F.month("closed_at")).orderBy("csat", "id")
w_channel_by_closed_at = Window.partitionBy("channel").orderBy("closed_at", "id")
w_all_by_agent_hired_on = Window.orderBy(F.col("agent_hired_on").desc_nulls_first(), "id")
w_channel_by_closed_at_moving = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(-2, Window.currentRow)
w_all_by_agent_hired_on_runnin = Window.orderBy(F.col("agent_hired_on").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_requester_id = Window.orderBy("requester_id", "id")
w_channel_by_closed_at_running = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_team = Window.partitionBy("agent_team")

valid_tickets = (
    df_tickets
    .filter(~F.col("channel").isin("email", "phone"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 60, 1).alias("minutes_closed_at_to_opened_at"),
    )
)

with_parent_agent = (
    valid_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

windowed = (
    with_parent_agent
    .select(
        "*",
        F.min("csat").over(w_csat).alias("csat_min_csat"),
        F.ntile(10).over(w_priority_closed_at_month_by).alias("csat_decile"),
        F.round(F.percent_rank().over(w_channel_by_closed_at), 3).alias("closed_at_percent_rank"),
        (F.col("requester_id") - F.lag("requester_id").over(w_all_by_agent_hired_on)).alias("requester_id_change"),
        F.round(F.avg("csat").over(w_csat), 2).alias("csat_avg_csat"),
        F.sum("csat").over(w_channel_by_closed_at_moving).alias("csat_sum_3"),
        F.sum("csat").over(w_all_by_agent_hired_on_runnin).alias("running_csat"),
        F.ntile(5).over(w_all_by_agent_hired_on).alias("agent_hired_on_quintile"),
        F.lead("agent_hired_on").over(w_all_by_requester_id).alias("next_agent_hired_on"),
        F.last("closed_at", ignorenulls=True).over(w_channel_by_closed_at_running).alias("last_known_closed_at"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_team), F.lit(0)), 4).alias("requester_id_share_of_agent_team"),
        F.lead("closed_at", 3).over(w_all_by_requester_id).alias("next3_closed_at"),
    )
)

derived = (
    windowed
    .select(
        "*",
        (F.col("requester_id") - F.col("closed_at_percent_rank")).alias("requester_id_minus_closed_at_percent_rank"),
        F.date_format(F.col("next3_closed_at"), "yyyy-MM").alias("next3_closed_at_label"),
        F.date_trunc("week", F.col("agent_hired_on")).alias("agent_hired_on_week"),
    )
)

summary = (
    derived
    .groupBy(
        "agent_hired_on_quintile",
        "channel",
        F.year("next_agent_hired_on").alias("next_agent_hired_on_year"),
    )
    .agg(
        F.count(F.when(F.upper(F.trim(F.col("agent_name"))).like("F%"), 1)).alias("n_upper_trim_agent_name_like_f"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("requester_id"), 2).alias("stddev_requester_id"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    summary
    .select(
        "agent_hired_on_quintile",
        "channel",
        "next_agent_hired_on_year",
        "n_upper_trim_agent_name_like_f",
        F.concat(F.col("channel"), F.lit("#"), F.col("agent_hired_on_quintile").cast("string")).alias("channel_ref"),
    )
)
