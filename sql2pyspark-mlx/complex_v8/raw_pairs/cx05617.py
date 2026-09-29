from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_by_csat = Window.partitionBy("csat").orderBy(F.col("csat").asc_nulls_last(), "id")
w_channel_csat_by_closed_at_fu = Window.partitionBy("channel", "csat").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc_nulls_first(), "id")
w_channel = Window.partitionBy("channel")
w_closed_at_to_date = Window.partitionBy(F.to_date("closed_at"))
w_agent_id_by_requester_id_run = Window.partitionBy("agent_id").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_opened_at_running = Window.orderBy(F.col("opened_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_requester_id_pri = Window.partitionBy("agent_id").orderBy("requester_id", "id").rowsBetween(-6, -1)
w_csat_by_csat_full = Window.partitionBy("csat").orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_requester_id_ful = Window.partitionBy("agent_id").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_agent_id_in_channel = Window.partitionBy("distinct_agent_id_in_channel")
w_distinct_channel_in_closed_a = Window.partitionBy("distinct_channel_in_closed_at_to_date", "csat_change").orderBy("closed_at", "id")
w_agent_id = Window.partitionBy("agent_id")
w_distinct_channel_in_closed_a_2 = Window.partitionBy("distinct_channel_in_closed_at_to_date").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = df_tickets.filter((F.col("csat") <= 4) & (~F.col("priority").isin("P3", "P4")))

with_details = (
    base.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.tags",
        "su.agent_name",
    )
)

windowed = (
    with_details
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "agent_name",
        (F.col("csat") - F.lag("csat").over(w_csat_by_csat)).alias("csat_change"),
        F.last("opened_at").over(w_channel_csat_by_closed_at_fu).alias("last_opened_at"),
        F.lag("closed_at").over(w_all_by_opened_at).alias("prev_closed_at"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.size(F.collect_set("channel").over(w_closed_at_to_date)).alias("distinct_channel_in_closed_at_to_date"),
        F.max("csat").over(w_agent_id_by_requester_id_run).alias("max_csat_so_far"),
        F.sum("requester_id").over(w_all_by_opened_at_running).alias("running_requester_id"),
        F.lag("closed_at", 2).over(w_all_by_opened_at).alias("prev2_closed_at"),
        F.sum("requester_id").over(w_closed_at_to_date).alias("closed_at_to_date_sum_requester_id"),
        F.sum("csat").over(w_channel).alias("channel_sum_csat"),
        F.round(F.avg("csat").over(w_agent_id_by_requester_id_pri), 2).alias("csat_avg_6_prior"),
        F.last("csat").over(w_csat_by_csat_full).alias("last_csat"),
        F.max("requester_id").over(w_channel).alias("channel_max_requester_id"),
        F.first("requester_id").over(w_agent_id_by_requester_id_ful).alias("first_requester_id"),
        F.size(F.collect_set("agent_id").over(w_channel)).alias("distinct_agent_id_in_channel"),
    )
)

scored = (
    windowed
    .select(
        "*",
        F.coalesce(F.col("csat_change"), F.lit(0)).alias("csat_change_filled"),
    )
)

sequenced = (
    scored
    .select(
        "id",
        "requester_id",
        "agent_id",
        "agent_name",
        "csat_change",
        "last_opened_at",
        "rows_in_channel",
        "max_csat_so_far",
        "running_requester_id",
        "prev2_closed_at",
        "closed_at_to_date_sum_requester_id",
        "last_csat",
        "channel_max_requester_id",
        "first_requester_id",
        "distinct_agent_id_in_channel",
        "csat_change_filled",
        F.count(F.lit(1)).over(w_distinct_agent_id_in_channel).alias("rows_in_distinct_agent_id_in_channel"),
        F.lead("last_opened_at").over(w_distinct_channel_in_closed_a).alias("next_last_opened_at"),
        F.sum("first_requester_id").over(w_distinct_agent_id_in_channel).alias("distinct_agent_id_in_channel_sum_first_requester"),
        F.size(F.collect_set("agent_id").over(w_distinct_agent_id_in_channel)).alias("distinct_agent_id_in_distinct_agent_id_in_channe"),
        F.round(F.col("channel_max_requester_id") / F.nullif(F.sum("channel_max_requester_id").over(w_distinct_agent_id_in_channel), F.lit(0)), 4).alias("channel_max_requester_id_share_of_distinct_agent"),
        F.round(F.col("csat_change") / F.nullif(F.sum("csat_change").over(w_agent_id), F.lit(0)), 4).alias("csat_change_share_of_agent_id"),
        F.nth_value("last_opened_at", 2).over(w_distinct_channel_in_closed_a_2).alias("second_last_opened_at"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "requester_id",
        "agent_name",
        "csat_change",
        "last_opened_at",
        "rows_in_channel",
        "max_csat_so_far",
        "running_requester_id",
        "closed_at_to_date_sum_requester_id",
        "last_csat",
        "channel_max_requester_id",
        "distinct_agent_id_in_channel",
        "csat_change_filled",
        "distinct_agent_id_in_channel_sum_first_requester",
        "distinct_agent_id_in_distinct_agent_id_in_channe",
        "channel_max_requester_id_share_of_distinct_agent",
    )
    .orderBy(F.col("requester_id").desc_nulls_first())
)
