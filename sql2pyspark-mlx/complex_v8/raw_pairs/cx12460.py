from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_agent_team_by_reque = Window.partitionBy("agent_id", "agent_team").orderBy(F.col("requester_id").desc(), "id")
w_agent_id_by_requester_id_tie = Window.partitionBy("agent_id").orderBy(F.col("requester_id").desc())
w_closed_at_year_agent_team = Window.partitionBy(F.year("closed_at"), "agent_team")
w_channel_closed_at_month_by_c = Window.partitionBy("channel", F.date_trunc("month", "closed_at")).orderBy(F.col("closed_at").desc(), "id")
w_agent_id_agent_team_by_reque_2 = Window.partitionBy("agent_id", "agent_team").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_channel = Window.partitionBy("agent_id", "channel")
w_agent_id_by_closed_at_runnin = Window.partitionBy("agent_id").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_team_by_requester_id_m = Window.partitionBy("agent_team").orderBy(F.col("requester_id_minus_csat").desc_nulls_first())
w_requester_id_dense_rank_dist = Window.partitionBy("requester_id_dense_rank", "distinct_agent_team_in_agent_id_channel").orderBy("first_known_requester_id_minus_csat_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_agent_team_in_agent = Window.partitionBy("distinct_agent_team_in_agent_id_channel", "closed_at_rank")
w_requester_id_dense_rank_dist_2 = Window.partitionBy("requester_id_dense_rank", "distinct_agent_team_in_agent_id_channel").orderBy("first_known_requester_id_minus_csat_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_team_by_running_count = Window.partitionBy("agent_team").orderBy(F.col("running_count").desc(), "id")
w_agent_team_by_running_count_2 = Window.partitionBy("agent_team").orderBy(F.col("running_count").desc())

recent_tickets = (
    df_tickets
    .filter(F.month(F.col("closed_at")).isin(1, 3))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
        (F.col("requester_id") - F.col("csat")).alias("requester_id_minus_csat"),
    )
)

joined_agent = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", "su.agent_name", F.col("su.team").alias("agent_team"))
)

with_windows = (
    joined_agent
    .select(
        "*",
        (F.col("requester_id_minus_csat") - F.lag("requester_id_minus_csat").over(w_agent_id_agent_team_by_reque)).alias("requester_id_minus_csat_change"),
        F.dense_rank().over(w_agent_id_by_requester_id_tie).alias("requester_id_dense_rank"),
        F.round(F.col("requester_id_minus_csat") / F.nullif(F.sum("requester_id_minus_csat").over(w_closed_at_year_agent_team), F.lit(0)), 4).alias("requester_id_minus_csat_share_of_closed_at_year_"),
        F.min("requester_id").over(w_closed_at_year_agent_team).alias("closed_at_year_agent_team_min_requester_id"),
        F.rank().over(w_channel_closed_at_month_by_c).alias("closed_at_rank"),
        F.first("requester_id_minus_csat", ignorenulls=True).over(w_agent_id_agent_team_by_reque_2).alias("first_known_requester_id_minus_csat"),
        F.size(F.collect_set("agent_team").over(w_agent_id_channel)).alias("distinct_agent_team_in_agent_id_channel"),
        (F.col("requester_id") - F.lag("requester_id").over(w_channel_closed_at_month_by_c)).alias("requester_id_change"),
        F.count(F.lit(1)).over(w_agent_id_agent_team_by_reque_2).alias("running_count"),
        F.lag("requester_id", 3).over(w_agent_id_agent_team_by_reque).alias("prev3_requester_id"),
        F.first("requester_id_minus_csat", ignorenulls=True).over(w_agent_id_by_closed_at_runnin).alias("first_known_requester_id_minus_csat_2"),
        F.sum("requester_id_minus_csat").over(w_agent_id_channel).alias("agent_id_channel_sum_requester_id_minus_csat"),
        F.dense_rank().over(w_agent_team_by_requester_id_m).alias("requester_id_minus_csat_dense_rank"),
    )
)

top_per_group = (
    with_windows
    .filter((F.col("requester_id_minus_csat_dense_rank") <= 5) & (F.col("first_known_requester_id_minus_csat") <= 18))
)

ranked = (
    top_per_group
    .select(
        "*",
        F.nth_value("running_count", 2).over(w_requester_id_dense_rank_dist).alias("second_running_count"),
        F.round(F.col("requester_id_change") / F.nullif(F.sum("requester_id_change").over(w_distinct_agent_team_in_agent), F.lit(0)), 4).alias("requester_id_change_share_of_distinct_agent_team"),
        F.last("requester_id_change", ignorenulls=True).over(w_requester_id_dense_rank_dist_2).alias("last_known_requester_id_change"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_distinct_agent_team_in_agent), F.lit(0)), 4).alias("requester_id_share_of_distinct_agent_team_in_age"),
        F.rank().over(w_agent_team_by_running_count).alias("running_count_rank"),
        F.dense_rank().over(w_agent_team_by_running_count_2).alias("running_count_dense_rank"),
    )
)

result = (
    ranked
    .filter(F.col("distinct_agent_team_in_agent_id_channel") <= 7)
    .select(
        "id",
        "requester_id",
        "channel",
        "tags",
        "requester_id_minus_csat",
        "agent_name",
        "agent_team",
        "requester_id_minus_csat_change",
        "requester_id_dense_rank",
        "requester_id_minus_csat_share_of_closed_at_year_",
        "closed_at_rank",
        "first_known_requester_id_minus_csat",
        "distinct_agent_team_in_agent_id_channel",
        "requester_id_change",
        "running_count",
        "prev3_requester_id",
        "first_known_requester_id_minus_csat_2",
        "agent_id_channel_sum_requester_id_minus_csat",
        "second_running_count",
        "requester_id_change_share_of_distinct_agent_team",
        "running_count_rank",
        "running_count_dense_rank",
        F.date_trunc("year", F.col("closed_at")).alias("closed_at_year"),
    )
    .orderBy("agent_team", F.col("requester_id_dense_rank").desc())
)
