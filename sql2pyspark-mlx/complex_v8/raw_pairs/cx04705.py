from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_requester_id_running = Window.orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_group_by_requester_i = Window.partitionBy("channel_group").orderBy(F.col("requester_id").desc(), "id")
w_agent_tier = Window.partitionBy("agent_tier")
w_opened_at_year_agent_hired_o = Window.partitionBy(F.year("opened_at"), F.date_trunc("month", "agent_hired_on")).orderBy("agent_hired_on")
w_opened_at_year_agent_hired_o_2 = Window.partitionBy(F.year("opened_at"), F.date_trunc("month", "agent_hired_on")).orderBy("agent_hired_on", "id")
w_channel_group_by_requester_i_2 = Window.partitionBy("channel_group").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_group_by_requester_i_3 = Window.partitionBy("channel_group").orderBy(F.col("requester_id").desc(), "id").rowsBetween(-6, -1)
w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc(), "id")
w_all_by_requester_id_ties = Window.orderBy(F.col("requester_id").desc())

base_tickets = (
    df_tickets
    .filter((F.col("channel") == "web") | (F.col("priority").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
        F.when(F.col("channel").isin("email", "phone"), "group_a").when(F.col("channel") == "chat", "group_b").otherwise("other").alias("channel_group"),
    )
)

enriched_agent = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.tags",
        "s.channel_group",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

windowed = (
    enriched_agent
    .select(
        "*",
        F.last("agent_hired_on", ignorenulls=True).over(w_all_by_requester_id_running).alias("last_known_agent_hired_on"),
        F.lead("opened_at").over(w_channel_group_by_requester_i).alias("next_opened_at"),
        F.size(F.collect_set("agent_team").over(w_agent_tier)).alias("distinct_agent_team_in_agent_tier"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_tier), F.lit(0)), 4).alias("requester_id_share_of_agent_tier"),
        F.dense_rank().over(w_opened_at_year_agent_hired_o).alias("agent_hired_on_dense_rank"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_opened_at_year_agent_hired_o_2))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_channel_group_by_requester_i)).alias("days_since_prev_agent_hired_on"),
        F.nth_value("agent_hired_on", 2).over(w_channel_group_by_requester_i_2).alias("second_agent_hired_on"),
        F.round(F.avg("requester_id").over(w_channel_group_by_requester_i_3), 2).alias("requester_id_avg_6_prior"),
        F.size(F.collect_set("agent_id").over(w_agent_tier)).alias("distinct_agent_id_in_agent_tier"),
        F.round(F.avg("requester_id").over(w_agent_tier), 2).alias("agent_tier_avg_requester_id"),
        F.lag("opened_at").over(w_all_by_requester_id).alias("prev_opened_at"),
        F.round(F.percent_rank().over(w_all_by_requester_id_ties), 3).alias("requester_id_percent_rank"),
    )
)

leaders = (
    windowed
    .filter(F.col("agent_hired_on_dense_rank") == 1)
    .drop("agent_hired_on_dense_rank")
)

result = (
    leaders
    .select(
        "id",
        "agent_id",
        "opened_at",
        "tags",
        "channel_group",
        "agent_tier",
        "agent_hired_on",
        "agent_team",
        "next_opened_at",
        "requester_id_share_of_agent_tier",
        "minutes_since_prev_opened_at",
        "days_since_prev_agent_hired_on",
        "requester_id_avg_6_prior",
        "distinct_agent_id_in_agent_tier",
        "agent_tier_avg_requester_id",
        "prev_opened_at",
        "requester_id_percent_rank",
    )
)
