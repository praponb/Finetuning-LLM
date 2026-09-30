from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_hired_on_year_opened_a = Window.partitionBy(F.year("agent_hired_on"), F.year("opened_at")).orderBy(F.col("requester_id").desc())
w_agent_hired_on_year_opened_a_2 = Window.partitionBy(F.year("agent_hired_on"), F.year("opened_at")).orderBy(F.col("requester_id").desc(), "id")
w_agent_id_by_agent_hired_on_t = Window.partitionBy("agent_id").orderBy("agent_hired_on")
w_agent_id = Window.partitionBy("agent_id")
w_agent_id_by_agent_hired_on_r = Window.partitionBy("agent_id").orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_agent_id_by_requeste = Window.partitionBy("channel", "agent_id").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_agent_hired_on = Window.partitionBy("agent_id").orderBy("agent_hired_on", "id")
w_agent_hired_on_year_opened_a_3 = Window.partitionBy(F.year("agent_hired_on"), F.year("opened_at")).orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_agent_id = Window.partitionBy("channel", "agent_id")
w_all_by_agent_hired_on = Window.orderBy(F.col("agent_hired_on").desc_nulls_first(), "id")
w_agent_hired_on_month_channel = Window.partitionBy(F.month("agent_hired_on"), "channel").orderBy(F.col("next3_requester_id").asc_nulls_last(), "id")
w_all_by_next3_requester_id = Window.orderBy(F.col("next3_requester_id").desc_nulls_first(), "id")

recent_tickets = (
    df_tickets
    .filter(F.col("channel") == "email")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
    )
)

joined = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.tags",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ranked = (
    joined
    .select(
        "*",
        F.round(F.percent_rank().over(w_agent_hired_on_year_opened_a), 3).alias("requester_id_percent_rank"),
        F.ntile(4).over(w_agent_hired_on_year_opened_a_2).alias("requester_id_quartile"),
        F.round(F.percent_rank().over(w_agent_id_by_agent_hired_on_t), 3).alias("agent_hired_on_percent_rank"),
        F.max("requester_id").over(w_agent_id).alias("agent_id_max_requester_id"),
        F.size(F.collect_set("channel").over(w_agent_id)).alias("distinct_channel_in_agent_id"),
        F.sum("requester_id").over(w_agent_id_by_agent_hired_on_r).alias("running_requester_id"),
        F.rank().over(w_agent_id_by_agent_hired_on_t).alias("agent_hired_on_rank"),
        F.sum("requester_id").over(w_channel_agent_id_by_requeste).alias("running_requester_id_2"),
        F.lead("requester_id", 3).over(w_agent_id_by_agent_hired_on).alias("next3_requester_id"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id), F.lit(0)), 4).alias("requester_id_share_of_agent_id"),
        F.sum("requester_id").over(w_agent_id).alias("agent_id_sum_requester_id"),
        F.first("agent_hired_on", ignorenulls=True).over(w_agent_hired_on_year_opened_a_3).alias("first_known_agent_hired_on"),
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.round(F.avg("next3_requester_id").over(w_channel_agent_id), 2).alias("channel_agent_id_avg_next3_requester_id"),
        F.dense_rank().over(w_all_by_agent_hired_on).alias("agent_hired_on_dense_rank"),
        F.ntile(4).over(w_agent_hired_on_month_channel).alias("next3_requester_id_quartile"),
        (F.col("rows_in_agent_id") - F.lag("rows_in_agent_id").over(w_all_by_next3_requester_id)).alias("rows_in_agent_id_change"),
        F.rank().over(w_agent_hired_on_month_channel).alias("next3_requester_id_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
        "agent_name",
        "agent_hired_on",
        "requester_id_quartile",
        "agent_hired_on_percent_rank",
        "distinct_channel_in_agent_id",
        "running_requester_id",
        "agent_hired_on_rank",
        "next3_requester_id",
        "rows_in_agent_id",
        "requester_id_share_of_agent_id",
        "agent_id_sum_requester_id",
        "channel_agent_id_avg_next3_requester_id",
        "agent_hired_on_dense_rank",
        "next3_requester_id_quartile",
        "rows_in_agent_id_change",
        "next3_requester_id_rank",
        F.coalesce(F.col("running_requester_id"), F.lit(0)).alias("running_requester_id_filled"),
    )
    .orderBy("agent_hired_on_rank")
)
