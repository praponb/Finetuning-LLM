from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority")
w_agent_hired_on_month_priorit = Window.partitionBy(F.date_trunc("month", "agent_hired_on"), "priority_len").orderBy(F.col("opened_at").desc())
w_priority_agent_tier = Window.partitionBy("priority", "agent_tier")
w_all_by_priority_len = Window.orderBy(F.col("priority_len").asc_nulls_last(), "id")
w_all_by_priority_len_ties = Window.orderBy(F.col("priority_len").asc_nulls_last())
w_all_by_priority_len_prior_6 = Window.orderBy(F.col("priority_len").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_priority_len_agent_id_by_ope = Window.partitionBy("priority_len", "agent_id").orderBy(F.col("opened_at").desc(), "id").rowsBetween(-2, -1)
w_priority_len_agent_id_by_ope_2 = Window.partitionBy("priority_len", "agent_id").orderBy(F.col("opened_at").desc(), "id")
w_agent_hired_on_year_opened_a = Window.partitionBy(F.year("agent_hired_on"), "opened_at_dense_rank").orderBy(F.col("priority_len_avg_2_prior").desc(), "id").rowsBetween(-1, Window.currentRow)
w_next3_priority_len = Window.partitionBy("next3_priority_len")
w_priority_agent_tier_by_agent = Window.partitionBy("priority", "agent_tier").orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_tickets = (
    df_tickets
    .filter((F.col("requester_id") < 19) | (F.col("opened_at") >= F.lit("2025-01-13 09:00:00").cast("timestamp")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

with_parent_agent = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.tags",
        "s.priority_len",
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_windows = (
    with_parent_agent
    .select(
        "*",
        F.round(F.col("priority_len") - F.avg("priority_len").over(w_priority), 2).alias("priority_len_vs_priority_avg"),
        F.dense_rank().over(w_agent_hired_on_month_priorit).alias("opened_at_dense_rank"),
        F.max("priority_len").over(w_priority_agent_tier).alias("priority_agent_tier_max_priority_len"),
        F.round(F.percent_rank().over(w_all_by_priority_len), 3).alias("priority_len_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_priority_len_ties), 3).alias("priority_len_percent_rank_2"),
        F.count(F.lit(1)).over(w_priority).alias("rows_in_priority"),
        F.round(F.avg("priority_len").over(w_all_by_priority_len_prior_6), 2).alias("priority_len_avg_6_prior"),
        F.rank().over(w_agent_hired_on_month_priorit).alias("opened_at_rank"),
        F.round(F.avg("priority_len").over(w_priority_len_agent_id_by_ope), 2).alias("priority_len_avg_2_prior"),
        F.min("priority_len").over(w_priority).alias("priority_min_priority_len"),
        F.lead("priority_len", 3).over(w_priority_len_agent_id_by_ope_2).alias("next3_priority_len"),
    )
)

first_rows = (
    with_windows
    .filter(F.col("opened_at_rank") <= 5)
    .drop("opened_at_rank")
)

with_trends = (
    first_rows
    .select(
        "id",
        "priority",
        "tags",
        "priority_len",
        "agent_name",
        "agent_hired_on",
        "opened_at_dense_rank",
        "priority_len_percent_rank",
        "rows_in_priority",
        "priority_min_priority_len",
        F.round(F.avg("priority_len").over(w_agent_hired_on_year_opened_a), 2).alias("priority_len_avg_2"),
        F.max("priority_len_avg_2_prior").over(w_next3_priority_len).alias("next3_priority_len_max_priority_len_avg_2_prior"),
        F.last("priority_len", ignorenulls=True).over(w_priority_agent_tier_by_agent).alias("last_known_priority_len"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "priority",
        "priority_len",
        "agent_hired_on",
        "opened_at_dense_rank",
        "priority_len_percent_rank",
        "rows_in_priority",
        "priority_len_avg_2",
        "next3_priority_len_max_priority_len_avg_2_prior",
        "last_known_priority_len",
        F.length(F.trim(F.col("agent_name"))).alias("agent_name_len"),
        F.substring(F.trim(F.col("agent_name")), 1, 2).alias("agent_name_prefix"),
    )
    .orderBy(
        F.col("agent_name_len").desc(),
        F.col("last_known_priority_len").desc_nulls_first(),
        "id",
    )
    .limit(20)
)
