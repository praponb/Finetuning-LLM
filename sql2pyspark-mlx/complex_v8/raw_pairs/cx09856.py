from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_year_agent_id_by_o = Window.partitionBy(F.year("opened_at"), "agent_id").orderBy(F.col("opened_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_opened_at_runnin = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_opened_at_full = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_year_agent_id_by_o_2 = Window.partitionBy(F.year("opened_at"), "agent_id").orderBy(F.col("opened_at").desc_nulls_first(), "id")
w_agent_id_by_opened_at = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), "id")
w_agent_id_by_opened_at_ties = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc())
w_agent_id_by_opened_at_ties_2 = Window.partitionBy("agent_id").orderBy("opened_at")
w_opened_at_month_agent_id_by = Window.partitionBy(F.month("opened_at"), "agent_id").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_known_opened_at_month = Window.partitionBy(F.date_trunc("month", "last_known_opened_at"))
w_next_opened_at_year_by_next = Window.partitionBy(F.year("next_opened_at")).orderBy("next_opened_at", "id")
w_agent_id_by_second_opened_at = Window.partitionBy("agent_id").orderBy(F.col("second_opened_at").desc(), "id")
w_max_next_opened_at_rank = Window.partitionBy("max_next_opened_at_rank")

scoped = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
        F.concat(F.col("priority"), F.lit("#"), F.col("csat").cast("string")).alias("priority_ref"),
    )
)

with_details = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.tags",
        "s.priority_ref",
        "su.agent_name",
    )
)

with_trends = (
    with_details
    .select(
        "id",
        "agent_id",
        "priority_ref",
        "agent_name",
        F.nth_value("opened_at", 2).over(w_opened_at_year_agent_id_by_o).alias("second_opened_at"),
        F.last("opened_at", ignorenulls=True).over(w_agent_id_by_opened_at_runnin).alias("last_known_opened_at"),
        F.last("opened_at").over(w_agent_id_by_opened_at_full).alias("last_opened_at"),
        F.lead("opened_at").over(w_opened_at_year_agent_id_by_o_2).alias("next_opened_at"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_agent_id_by_opened_at))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.round(F.percent_rank().over(w_agent_id_by_opened_at_ties), 3).alias("opened_at_percent_rank"),
        F.dense_rank().over(w_agent_id_by_opened_at_ties_2).alias("opened_at_dense_rank"),
        F.last("opened_at", ignorenulls=True).over(w_opened_at_month_agent_id_by).alias("last_known_opened_at_2"),
    )
)

labeled = (
    with_trends
    .select(
        "id",
        "agent_id",
        "priority_ref",
        "agent_name",
        "second_opened_at",
        "last_known_opened_at",
        "next_opened_at",
        "minutes_since_prev_opened_at",
        "opened_at_percent_rank",
        "opened_at_dense_rank",
        "last_known_opened_at_2",
        F.concat(F.col("agent_name"), F.lit("#"), F.col("id").cast("string")).alias("agent_name_ref"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("next_opened_at")).alias("days_since_next_opened_at"),
    )
)

with_trends_2 = (
    labeled
    .select(
        "id",
        "agent_id",
        "agent_name",
        "last_known_opened_at",
        "last_known_opened_at_2",
        F.max("opened_at_dense_rank").over(w_last_known_opened_at_month).alias("last_known_opened_at_month_max_opened_at_dense_r"),
        F.round(F.col("opened_at_percent_rank") - F.avg("opened_at_percent_rank").over(w_last_known_opened_at_month), 2).alias("opened_at_percent_rank_vs_last_known_opened_at_m"),
        F.rank().over(w_next_opened_at_year_by_next).alias("next_opened_at_rank"),
        F.lag("last_known_opened_at_2").over(w_agent_id_by_second_opened_at).alias("prev_last_known_opened_at_2"),
    )
)

stats = (
    with_trends_2
    .groupBy(
        F.to_date("prev_last_known_opened_at_2").alias("prev_last_known_opened_at_2_day"),
        F.date_format("last_known_opened_at", "yyyy-MM").alias("last_known_opened_at_ym"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.sum("last_known_opened_at_month_max_opened_at_dense_r").alias("total_last_known_opened_at_month_max_opened_at_d"),
        F.max("next_opened_at_rank").alias("max_next_opened_at_rank"),
    )
)

with_windows = (
    stats
    .select(
        "prev_last_known_opened_at_2_day",
        "last_known_opened_at_ym",
        "cnt",
        "distinct_agent_ids",
        "max_next_opened_at_rank",
        F.sum("distinct_agent_ids").over(w_max_next_opened_at_rank).alias("max_next_opened_at_rank_sum_distinct_agent_ids"),
        F.count(F.lit(1)).over(w_max_next_opened_at_rank).alias("rows_in_max_next_opened_at_rank"),
        F.min("distinct_agent_ids").over(w_max_next_opened_at_rank).alias("max_next_opened_at_rank_min_distinct_agent_ids"),
    )
)

result = (
    with_windows
    .filter(F.col("cnt") >= 2)
    .select(
        "prev_last_known_opened_at_2_day",
        "last_known_opened_at_ym",
        "distinct_agent_ids",
        "max_next_opened_at_rank",
        "max_next_opened_at_rank_sum_distinct_agent_ids",
        "max_next_opened_at_rank_min_distinct_agent_ids",
        (F.col("cnt") - F.col("max_next_opened_at_rank")).alias("cnt_minus_max_next_opened_at_rank"),
    )
    .orderBy("cnt_minus_max_next_opened_at_rank")
)
