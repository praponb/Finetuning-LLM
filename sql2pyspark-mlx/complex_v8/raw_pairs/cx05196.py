from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_year_by_opened_at = Window.partitionBy(F.year("opened_at")).orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_hired_on_year_by_close = Window.partitionBy(F.year("agent_hired_on")).orderBy(F.col("closed_at").asc_nulls_last(), "id")
w_closed_at_year_by_closed_at = Window.partitionBy(F.year("closed_at")).orderBy(F.col("closed_at").desc(), "id")
w_all_by_agent_hired_on = Window.orderBy(F.col("agent_hired_on").desc(), "id")
w_all_by_agent_hired_on_runnin = Window.orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_year_by_opened_at_2 = Window.partitionBy(F.year("opened_at")).orderBy(F.col("opened_at").asc_nulls_last())
w_priority_agent_team_by_opene = Window.partitionBy("priority", "agent_team").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_closed_at_dense_rank_closed = Window.partitionBy("closed_at_dense_rank", "closed_at_rank").orderBy(F.col("first_opened_at").desc(), "id")
w_first_opened_at_month_by_pre = Window.partitionBy(F.month("first_opened_at")).orderBy("prev_agent_hired_on", "id")
w_first_known_agent_hired_on_m = Window.partitionBy(F.date_trunc("month", "first_known_agent_hired_on"))
w_closed_at_dense_rank_closed_2 = Window.partitionBy("closed_at_dense_rank", "closed_at_rank").orderBy(F.col("first_opened_at").desc(), "id").rowsBetween(-2, -1)

filtered = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-01-26 18:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
    )
)

combined_details_agent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel_ref",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

windowed = (
    combined_details_agent
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel_ref",
        "agent_name",
        F.last("closed_at", ignorenulls=True).over(w_opened_at_year_by_opened_at).alias("last_known_closed_at"),
        F.rank().over(w_agent_hired_on_year_by_close).alias("closed_at_rank"),
        F.row_number().over(w_closed_at_year_by_closed_at).alias("seq_no"),
        F.lag("opened_at", 3).over(w_all_by_agent_hired_on).alias("prev3_opened_at"),
        F.lag("agent_hired_on").over(w_agent_hired_on_year_by_close).alias("prev_agent_hired_on"),
        F.first("agent_hired_on", ignorenulls=True).over(w_all_by_agent_hired_on_runnin).alias("first_known_agent_hired_on"),
        F.first("agent_hired_on", ignorenulls=True).over(w_opened_at_year_by_opened_at).alias("first_known_agent_hired_on_2"),
        F.round(F.percent_rank().over(w_opened_at_year_by_opened_at_2), 3).alias("opened_at_percent_rank"),
        F.first("opened_at").over(w_priority_agent_team_by_opene).alias("first_opened_at"),
        F.dense_rank().over(w_agent_hired_on_year_by_close).alias("closed_at_dense_rank"),
    )
)

tagged = (
    windowed
    .select(
        "id",
        "closed_at",
        "priority",
        "channel_ref",
        "closed_at_rank",
        "seq_no",
        "prev3_opened_at",
        "prev_agent_hired_on",
        "first_known_agent_hired_on",
        "first_known_agent_hired_on_2",
        "first_opened_at",
        "closed_at_dense_rank",
        F.concat_ws(": ", F.col("channel_ref"), F.col("agent_name")).alias("channel_ref_agent_name_label"),
    )
)

windowed_2 = (
    tagged
    .select(
        "*",
        F.row_number().over(w_closed_at_dense_rank_closed).alias("seq_no_2"),
        F.rank().over(w_first_opened_at_month_by_pre).alias("prev_agent_hired_on_rank"),
        F.round(F.col("closed_at_rank") / F.nullif(F.sum("closed_at_rank").over(w_first_known_agent_hired_on_m), F.lit(0)), 4).alias("closed_at_rank_share_of_first_known_agent_hired_"),
        F.round(F.avg("closed_at_rank").over(w_closed_at_dense_rank_closed_2), 2).alias("closed_at_rank_avg_2_prior"),
        F.ntile(10).over(w_first_opened_at_month_by_pre).alias("prev_agent_hired_on_decile"),
        F.round(F.percent_rank().over(w_first_opened_at_month_by_pre), 3).alias("prev_agent_hired_on_percent_rank"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "priority",
        "channel_ref",
        "prev3_opened_at",
        "prev_agent_hired_on",
        "first_known_agent_hired_on",
        "first_known_agent_hired_on_2",
        "first_opened_at",
        "channel_ref_agent_name_label",
        "seq_no_2",
        "closed_at_rank_share_of_first_known_agent_hired_",
        "closed_at_rank_avg_2_prior",
        "prev_agent_hired_on_decile",
        "prev_agent_hired_on_percent_rank",
    )
    .orderBy(F.col("seq_no_2").desc_nulls_first(), "prev3_opened_at")
)
