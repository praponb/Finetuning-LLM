from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_closed_at = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_tier_agent_id_by_close = Window.partitionBy("agent_tier", "agent_id").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_agent_team_by_agent = Window.partitionBy("agent_id", "agent_team").orderBy(F.col("agent_hired_on").desc())
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")
w_agent_tier_agent_id_by_close_2 = Window.partitionBy("agent_tier", "agent_id").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_closed_at_full = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_agent_team_by_agent_2 = Window.partitionBy("agent_id", "agent_team").orderBy(F.col("agent_hired_on").desc(), "id")
w_agent_tier_agent_id_by_close_3 = Window.partitionBy("agent_tier", "agent_id").orderBy(F.col("closed_at").desc(), "id")
w_agent_id_by_closed_at_full = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_team_agent_tier_by_age = Window.partitionBy("agent_team", "agent_tier").orderBy(F.col("agent_hired_on").desc(), "id")
w_all_by_second_agent_hired_on = Window.orderBy(F.col("second_agent_hired_on").desc(), "id").rowsBetween(-1, -1)
w_all_by_second_agent_hired_on_2 = Window.orderBy(F.col("second_agent_hired_on").desc(), "id")
w_all_by_second_agent_hired_on_3 = Window.orderBy(F.col("second_agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_hired_on_month = Window.partitionBy(F.date_trunc("month", "agent_hired_on"))
w_all_by_agent_hired_on_percen = Window.orderBy(F.col("agent_hired_on_percent_rank").desc(), "id")
w_all_by_agent_hired_on_percen_2 = Window.orderBy(F.col("agent_hired_on_percent_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tickets
    .filter(F.col("requester_id") < 19)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.when(F.col("channel").isin("chat", "web"), "group_a").when(F.col("channel") == "phone", "group_b").otherwise("other").alias("channel_group"),
    )
)

combined_details_agent = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

metrics = (
    combined_details_agent
    .select(
        "*",
        F.rank().over(w_agent_id_by_closed_at).alias("closed_at_rank"),
        F.first("agent_hired_on").over(w_agent_tier_agent_id_by_close).alias("first_agent_hired_on"),
        F.dense_rank().over(w_agent_id_agent_team_by_agent).alias("agent_hired_on_dense_rank"),
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_all_by_closed_at)).alias("days_since_prev_agent_hired_on"),
        F.last("closed_at", ignorenulls=True).over(w_agent_tier_agent_id_by_close_2).alias("last_known_closed_at"),
        F.last("agent_hired_on").over(w_all_by_closed_at_full).alias("last_agent_hired_on"),
        F.ntile(4).over(w_agent_id_agent_team_by_agent_2).alias("agent_hired_on_quartile"),
        F.row_number().over(w_agent_tier_agent_id_by_close_3).alias("closed_at_seq"),
        F.nth_value("agent_hired_on", 2).over(w_agent_id_by_closed_at_full).alias("second_agent_hired_on"),
        F.round(F.percent_rank().over(w_agent_id_agent_team_by_agent_2), 3).alias("agent_hired_on_percent_rank"),
        F.nth_value("closed_at", 2).over(w_agent_id_by_closed_at_full).alias("second_closed_at"),
        F.lag("closed_at", 3).over(w_agent_team_agent_tier_by_age).alias("prev3_closed_at"),
    )
)

labeled = (
    metrics
    .filter(F.col("second_closed_at") >= F.lit("2025-02-02 03:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat_ws("-", F.col("agent_tier"), F.col("agent_team")).alias("agent_tier_agent_team_label"),
        F.date_format(F.col("closed_at"), "MMM yyyy").alias("closed_at_label"),
    )
)

windowed = (
    labeled
    .select(
        "*",
        F.max("agent_hired_on_percent_rank").over(w_all_by_second_agent_hired_on).alias("agent_hired_on_percent_rank_max_1_prior"),
        F.lead("closed_at").over(w_all_by_second_agent_hired_on_2).alias("next_closed_at"),
        F.count(F.lit(1)).over(w_all_by_second_agent_hired_on_3).alias("running_count"),
        F.max("closed_at_seq").over(w_agent_hired_on_month).alias("agent_hired_on_month_max_closed_at_seq"),
        F.row_number().over(w_all_by_agent_hired_on_percen).alias("seq_no"),
        F.round(F.col("agent_hired_on_quartile") - F.avg("agent_hired_on_quartile").over(w_agent_hired_on_month), 2).alias("agent_hired_on_quartile_vs_agent_hired_on_month_"),
        F.last("second_agent_hired_on", ignorenulls=True).over(w_all_by_agent_hired_on_percen_2).alias("last_known_second_agent_hired_on"),
    )
)

result = (
    windowed
    .select(
        "id",
        "agent_id",
        "closed_at",
        "agent_team",
        "agent_tier",
        "agent_hired_on_dense_rank",
        "last_known_closed_at",
        "last_agent_hired_on",
        "second_agent_hired_on",
        "agent_hired_on_percent_rank",
        "second_closed_at",
        "prev3_closed_at",
        "next_closed_at",
        "running_count",
        "seq_no",
        "agent_hired_on_quartile_vs_agent_hired_on_month_",
        "last_known_second_agent_hired_on",
    )
)
