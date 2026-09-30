from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_month = Window.partitionBy(F.date_trunc("month", "opened_at"))
w_priority_channel_by_csat_run = Window.partitionBy("priority", "channel").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_csat = Window.orderBy(F.col("csat").desc(), "id")
w_priority_channel_by_csat = Window.partitionBy("priority", "channel").orderBy("csat", "id")
w_priority_channel_by_csat_ful = Window.partitionBy("priority", "channel").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_csat_ties = Window.partitionBy("agent_id").orderBy("csat")

base = (
    df_tickets
    .filter(F.col("priority").isNotNull())
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
    )
)

enriched_agent = (
    base.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

windowed = (
    enriched_agent
    .select(
        "id",
        "opened_at",
        "priority",
        "channel",
        "agent_tier",
        "agent_hired_on",
        F.count(F.lit(1)).over(w_opened_at_month).alias("rows_in_opened_at_month"),
        F.first("csat", ignorenulls=True).over(w_priority_channel_by_csat_run).alias("first_known_csat"),
        F.first("agent_hired_on", ignorenulls=True).over(w_priority_channel_by_csat_run).alias("first_known_agent_hired_on"),
        F.row_number().over(w_all_by_csat).alias("rn"),
        F.row_number().over(w_priority_channel_by_csat).alias("rn_2"),
        F.round(F.col("csat") - F.avg("csat").over(w_opened_at_month), 2).alias("csat_vs_opened_at_month_avg"),
        F.nth_value("agent_hired_on", 2).over(w_priority_channel_by_csat_ful).alias("second_agent_hired_on"),
        F.lead("csat", 2).over(w_priority_channel_by_csat).alias("next2_csat"),
        F.round(F.avg("csat").over(w_opened_at_month), 2).alias("opened_at_month_avg_csat"),
        F.rank().over(w_agent_id_by_csat_ties).alias("csat_rank"),
        (F.col("csat") - F.lag("csat").over(w_all_by_csat)).alias("csat_change"),
    )
)

top_per_group = (
    windowed
    .filter(F.col("csat_rank") == 1)
    .drop("csat_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "opened_at",
        "priority",
        "channel",
        "agent_tier",
        "agent_hired_on",
        "rows_in_opened_at_month",
        "first_known_csat",
        "first_known_agent_hired_on",
        "rn",
        "rn_2",
        "csat_vs_opened_at_month_avg",
        "second_agent_hired_on",
        "next2_csat",
        "csat_change",
        F.concat(F.col("agent_tier"), F.lit("#"), F.col("id").cast("string")).alias("agent_tier_ref"),
    )
    .orderBy(F.col("opened_at").asc_nulls_last(), "id")
    .limit(10)
)
