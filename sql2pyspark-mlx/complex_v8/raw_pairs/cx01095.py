from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_agent_hired_on = Window.orderBy("agent_hired_on", "id")
w_channel_by_agent_hired_on = Window.partitionBy("channel").orderBy(F.col("agent_hired_on").desc(), "id")
w_agent_id_agent_hired_on_mont = Window.partitionBy("agent_id", F.date_trunc("month", "agent_hired_on"))
w_agent_team_by_opened_at_ties = Window.partitionBy("agent_team").orderBy(F.col("opened_at").asc_nulls_last())
w_all_by_agent_hired_on_ties = Window.orderBy("agent_hired_on")
w_agent_id = Window.partitionBy("agent_id")
w_channel_by_agent_hired_on_pr = Window.partitionBy("channel").orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(-6, -1)
w_all_by_agent_hired_on_full = Window.orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_hired_on_dense_rank_ag = Window.partitionBy("agent_hired_on_dense_rank", "agent_hired_on_rank")
w_all_by_agent_hired_on_quarti = Window.orderBy(F.col("agent_hired_on_quartile_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_hired_on_dense_rank_by = Window.partitionBy("agent_hired_on_dense_rank").orderBy("agent_hired_on_quartile_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_hired_on_dense_rank = Window.partitionBy("agent_hired_on_dense_rank")
w_first_agent_hired_on_month_a = Window.partitionBy(F.month("first_agent_hired_on"), "agent_hired_on_dense_rank").orderBy(F.col("agent_hired_on").desc(), "id")

valid_tickets = (
    df_tickets
    .filter(F.col("csat").between(2, 4))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

with_parent = (
    valid_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.days_since_closed_at",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

metrics = (
    with_parent
    .select(
        "*",
        F.row_number().over(w_all_by_agent_hired_on).alias("seq_no"),
        F.lag("days_since_closed_at", 2).over(w_channel_by_agent_hired_on).alias("prev2_days_since_closed_at"),
        F.count(F.lit(1)).over(w_agent_id_agent_hired_on_mont).alias("rows_in_agent_id_agent_hired_on_month"),
        F.round(F.cume_dist().over(w_agent_team_by_opened_at_ties), 3).alias("opened_at_cume_dist"),
        F.ntile(4).over(w_channel_by_agent_hired_on).alias("agent_hired_on_quartile"),
        F.rank().over(w_all_by_agent_hired_on_ties).alias("agent_hired_on_rank"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.dense_rank().over(w_all_by_agent_hired_on_ties).alias("agent_hired_on_dense_rank"),
        F.min("days_since_closed_at").over(w_agent_id_agent_hired_on_mont).alias("agent_id_agent_hired_on_month_min_days_since_clo"),
        F.max("days_since_closed_at").over(w_channel_by_agent_hired_on_pr).alias("days_since_closed_at_max_6_prior"),
        F.min("days_since_closed_at").over(w_agent_id).alias("agent_id_min_days_since_closed_at"),
        F.first("agent_hired_on").over(w_all_by_agent_hired_on_full).alias("first_agent_hired_on"),
    )
)

tagged = (
    metrics
    .select(
        "id",
        "agent_id",
        "opened_at",
        "days_since_closed_at",
        "agent_hired_on",
        "rows_in_agent_id_agent_hired_on_month",
        "opened_at_cume_dist",
        "agent_hired_on_quartile",
        "agent_hired_on_rank",
        "agent_hired_on_dense_rank",
        "days_since_closed_at_max_6_prior",
        "agent_id_min_days_since_closed_at",
        "first_agent_hired_on",
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
        (F.floor(F.col("agent_hired_on_quartile") / 2) * 2).alias("agent_hired_on_quartile_bucket"),
        F.concat_ws("-", F.col("agent_team"), F.col("channel")).alias("agent_team_channel_label"),
    )
)

sequenced = (
    tagged
    .select(
        "*",
        F.count(F.lit(1)).over(w_agent_hired_on_dense_rank_ag).alias("rows_in_agent_hired_on_dense_rank_agent_hired_on"),
        F.sum("agent_hired_on_quartile").over(w_all_by_agent_hired_on_quarti).alias("running_agent_hired_on_quartile"),
        F.count(F.lit(1)).over(w_agent_hired_on_dense_rank_by).alias("running_count"),
        F.size(F.collect_set("agent_id").over(w_agent_hired_on_dense_rank)).alias("distinct_agent_id_in_agent_hired_on_dense_rank"),
        F.last("opened_at", ignorenulls=True).over(w_all_by_agent_hired_on_quarti).alias("last_known_opened_at"),
        F.rank().over(w_first_agent_hired_on_month_a).alias("agent_hired_on_rank_2"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "agent_id",
        "opened_at",
        "days_since_closed_at",
        "agent_hired_on",
        "opened_at_cume_dist",
        "agent_hired_on_quartile",
        "agent_hired_on_rank",
        "agent_hired_on_dense_rank",
        "days_since_closed_at_max_6_prior",
        "agent_id_min_days_since_closed_at",
        "first_agent_hired_on",
        "channel_ref",
        "agent_hired_on_quartile_bucket",
        "agent_team_channel_label",
        "rows_in_agent_hired_on_dense_rank_agent_hired_on",
        "running_agent_hired_on_quartile",
        "running_count",
        "last_known_opened_at",
        "agent_hired_on_rank_2",
        F.coalesce(F.col("days_since_closed_at"), F.lit(0)).alias("days_since_closed_at_filled"),
    )
    .orderBy(
        F.col("agent_hired_on_rank").asc_nulls_last(),
        F.col("last_known_opened_at").desc(),
    )
)
