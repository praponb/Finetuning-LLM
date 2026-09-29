from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_days_since_opened_at = Window.orderBy(F.col("days_since_opened_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_group_agent_team_by = Window.partitionBy("priority_group", "agent_team").orderBy("days_since_opened_at")
w_agent_team_agent_id = Window.partitionBy("agent_team", "agent_id")
w_agent_tier_agent_id_by_reque = Window.partitionBy("agent_tier", "agent_id").orderBy("requester_id", "id")
w_closed_at_year_agent_tier = Window.partitionBy(F.year("closed_at"), "agent_tier")
w_priority_group_agent_team_by_2 = Window.partitionBy("priority_group", "agent_team").orderBy("days_since_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_tier_closed_at_month_b = Window.partitionBy("agent_tier", F.month("closed_at")).orderBy("closed_at", "id")
w_agent_id = Window.partitionBy("agent_id")
w_agent_tier_agent_id_by_reque_2 = Window.partitionBy("agent_tier", "agent_id").orderBy("requester_id", "id").rowsBetween(-3, Window.currentRow)
w_n_rows = Window.partitionBy("n_rows")
w_n_rows_by_total_agent_id_max = Window.partitionBy("n_rows").orderBy("total_agent_id_max_days_since_opened_at", "second_closed_at_month").rowsBetween(-3, -1)
w_n_rows_by_total_agent_id_max_2 = Window.partitionBy("n_rows").orderBy("total_agent_id_max_days_since_opened_at", "second_closed_at_month")

prepared = (
    df_tickets
    .filter((F.to_date(F.col("closed_at")) < F.lit("2025-03-09").cast("date")) | (F.col("csat") >= 2))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.when(F.col("priority").isin("P2", "P4"), "group_a").when(F.col("priority") == "P3", "group_b").otherwise("other").alias("priority_group"),
    )
)

combined_details = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.team") == "VIP")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.days_since_opened_at",
        "s.priority_group",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.nth_value("closed_at", 2).over(w_all_by_days_since_opened_at).alias("second_closed_at"),
        F.round(F.percent_rank().over(w_priority_group_agent_team_by), 3).alias("days_since_opened_at_percent_rank"),
        F.min("requester_id").over(w_agent_team_agent_id).alias("agent_team_agent_id_min_requester_id"),
        (F.col("requester_id") - F.lag("requester_id").over(w_agent_tier_agent_id_by_reque)).alias("requester_id_change"),
        F.count(F.lit(1)).over(w_closed_at_year_agent_tier).alias("rows_in_closed_at_year_agent_tier"),
        F.nth_value("days_since_opened_at", 2).over(w_priority_group_agent_team_by_2).alias("second_days_since_opened_at"),
        F.lead("days_since_opened_at").over(w_agent_tier_closed_at_month_b).alias("next_days_since_opened_at"),
        F.max("days_since_opened_at").over(w_agent_id).alias("agent_id_max_days_since_opened_at"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_team_agent_id), F.lit(0)), 4).alias("requester_id_share_of_agent_team_agent_id"),
        F.round(F.avg("days_since_opened_at").over(w_agent_tier_agent_id_by_reque_2), 2).alias("days_since_opened_at_avg_4"),
        F.round(F.avg("days_since_opened_at").over(w_closed_at_year_agent_tier), 2).alias("closed_at_year_agent_tier_avg_days_since_opened_"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_closed_at_year_agent_tier), 2).alias("requester_id_vs_closed_at_year_agent_tier_avg"),
    )
)

labeled = (
    windowed
    .filter(F.col("requester_id_share_of_agent_team_agent_id").between(0.3, 0.9))
    .withColumn("priority_group_clean", F.initcap(F.trim(F.col("priority_group"))))
)

per_group = (
    labeled
    .filter(F.col("second_closed_at") < F.lit("2025-02-24 13:00:00").cast("timestamp"))
    .groupBy(
        F.date_trunc("month", "second_closed_at").alias("second_closed_at_month"),
    )
    .agg(
        F.round(F.sum("days_since_opened_at_avg_4"), 2).alias("total_days_since_opened_at_avg_4"),
        F.sum("agent_id_max_days_since_opened_at").alias("total_agent_id_max_days_since_opened_at"),
        F.count("*").alias("n_rows"),
        F.sum("second_days_since_opened_at").alias("total_second_days_since_opened_at"),
        F.avg("requester_id_share_of_agent_team_agent_id").alias("avg_requester_id_share_of_agent_team_agent_id_ch"),
    )
    .filter(F.col("avg_requester_id_share_of_agent_team_agent_id_ch") > 0.1)
    .drop("avg_requester_id_share_of_agent_team_agent_id_ch")
)

windowed_2 = (
    per_group
    .select(
        "second_closed_at_month",
        "total_days_since_opened_at_avg_4",
        "n_rows",
        "total_second_days_since_opened_at",
        F.round(F.col("n_rows") / F.nullif(F.sum("n_rows").over(w_n_rows), F.lit(0)), 4).alias("n_rows_share_of_n_rows"),
        F.max("n_rows").over(w_n_rows_by_total_agent_id_max).alias("n_rows_max_3_prior"),
        F.round(F.cume_dist().over(w_n_rows_by_total_agent_id_max_2), 3).alias("total_agent_id_max_days_since_opened_at_cume_dis"),
    )
)

result = (
    windowed_2
    .select(
        "second_closed_at_month",
        "n_rows",
        "total_second_days_since_opened_at",
        "n_rows_share_of_n_rows",
        "total_agent_id_max_days_since_opened_at_cume_dis",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("second_closed_at_month")).alias("days_since_second_closed_at_month"),
        (F.floor(F.col("n_rows_max_3_prior") / 5) * 5).alias("n_rows_max_3_prior_bucket"),
    )
    .orderBy(
        F.col("n_rows_max_3_prior_bucket").asc_nulls_last(),
        F.col("days_since_second_closed_at_month").desc(),
    )
)
