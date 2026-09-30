from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_csat_running = Window.orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat = Window.partitionBy("csat")
w_agent_id = Window.partitionBy("agent_id")
w_all_by_csat = Window.orderBy("csat", "id")
w_csat_agent_hired_on_year_by = Window.partitionBy("csat", F.year("agent_hired_on")).orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_hired_on_month_csat_by = Window.partitionBy(F.date_trunc("month", "agent_hired_on"), "csat").orderBy(F.col("agent_hired_on").desc())
w_agent_hired_on_month_by_requ = Window.partitionBy(F.month("agent_hired_on")).orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_csat_full = Window.orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_hired_on_month_csat_by_2 = Window.partitionBy(F.date_trunc("month", "agent_hired_on"), "csat").orderBy(F.col("agent_hired_on").desc(), "id")
w_all_by_second_agent_hired_on = Window.orderBy(F.col("second_agent_hired_on").desc_nulls_first(), "id")
w_csat_decile_agent_id_by_firs = Window.partitionBy("csat_decile", "agent_id").orderBy("first_agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_second_agent_hired_on_2 = Window.orderBy(F.col("second_agent_hired_on").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_csat_decile_agent_id_by_firs_2 = Window.partitionBy("csat_decile", "agent_id").orderBy("first_agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
    )
)

enriched_agent = (
    prepared_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        "s.requester_id_per_csat",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_windows = (
    enriched_agent
    .select(
        "*",
        F.max("csat").over(w_all_by_csat_running).alias("max_csat_so_far"),
        F.round(F.col("csat") - F.avg("csat").over(w_csat), 2).alias("csat_vs_csat_avg"),
        F.max("csat").over(w_agent_id).alias("agent_id_max_csat"),
        F.ntile(10).over(w_all_by_csat).alias("csat_decile"),
        F.nth_value("agent_hired_on", 2).over(w_csat_agent_hired_on_year_by).alias("second_agent_hired_on"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_id), F.lit(0)), 4).alias("csat_share_of_agent_id"),
        F.round(F.cume_dist().over(w_agent_hired_on_month_csat_by), 3).alias("agent_hired_on_cume_dist"),
        F.min("csat").over(w_agent_id).alias("agent_id_min_csat"),
        F.min("requester_id").over(w_agent_id).alias("agent_id_min_requester_id"),
        F.first("agent_hired_on").over(w_agent_hired_on_month_by_requ).alias("first_agent_hired_on"),
        F.nth_value("csat", 2).over(w_all_by_csat_full).alias("second_csat"),
        F.sum("csat").over(w_agent_id).alias("agent_id_sum_csat"),
        F.round(F.percent_rank().over(w_agent_hired_on_month_csat_by_2), 3).alias("agent_hired_on_percent_rank"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "*",
        F.lag("agent_hired_on_percent_rank", 3).over(w_all_by_second_agent_hired_on).alias("prev3_agent_hired_on_percent_rank"),
        F.nth_value("agent_id_sum_csat", 2).over(w_csat_decile_agent_id_by_firs).alias("second_agent_id_sum_csat"),
        F.first("max_csat_so_far").over(w_csat_decile_agent_id_by_firs).alias("first_max_csat_so_far"),
        F.sum("csat_share_of_agent_id").over(w_all_by_second_agent_hired_on_2).alias("csat_share_of_agent_id_sum_4"),
        F.first("second_csat", ignorenulls=True).over(w_csat_decile_agent_id_by_firs_2).alias("first_known_second_csat"),
        F.max("csat_share_of_agent_id").over(w_agent_id).alias("agent_id_max_csat_share_of_agent_id"),
        F.min("agent_id_max_csat").over(w_agent_id).alias("agent_id_min_agent_id_max_csat"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "closed_at",
        "csat",
        "requester_id_per_csat",
        "max_csat_so_far",
        "csat_vs_csat_avg",
        "agent_id_max_csat",
        "csat_decile",
        "second_agent_hired_on",
        "csat_share_of_agent_id",
        "agent_hired_on_cume_dist",
        "agent_id_min_csat",
        "agent_id_min_requester_id",
        "first_agent_hired_on",
        "agent_hired_on_percent_rank",
        "prev3_agent_hired_on_percent_rank",
        "second_agent_id_sum_csat",
        "first_max_csat_so_far",
        "csat_share_of_agent_id_sum_4",
        "first_known_second_csat",
        "agent_id_max_csat_share_of_agent_id",
        F.round(F.col("csat_vs_csat_avg") * 1.07, 2).alias("csat_vs_csat_avg_adj"),
    )
    .orderBy(F.col("csat_vs_csat_avg").desc(), F.col("id").desc())
)
