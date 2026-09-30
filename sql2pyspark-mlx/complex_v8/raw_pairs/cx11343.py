from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_priority_by_request = Window.partitionBy("agent_id", "priority").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_hired_on_month_priorit = Window.partitionBy(F.date_trunc("month", "agent_hired_on"), "priority_group")
w_agent_id_priority_by_request_2 = Window.partitionBy("agent_id", "priority").orderBy(F.col("requester_id").desc(), "id").rowsBetween(-3, -1)
w_agent_id_priority_by_request_3 = Window.partitionBy("agent_id", "priority").orderBy(F.col("requester_id").desc())
w_agent_id_by_closed_at_runnin = Window.partitionBy("agent_id").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_agent_hired_on = Window.orderBy("agent_hired_on", "id")
w_all_by_agent_hired_on_ties = Window.orderBy("agent_hired_on")
w_agent_id_priority = Window.partitionBy("agent_id", "priority")
w_agent_team_len_opened_at_mon = Window.partitionBy("agent_team_len", F.month("opened_at")).orderBy(F.col("second_opened_at").desc(), "id")
w_distinct_priority_in_agent_h = Window.partitionBy("distinct_priority_in_agent_hired_on_month_priori", "agent_id")

base = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        F.when(F.col("priority").isin("P3", "P4"), "group_a").when(F.col("priority") == "P2", "group_b").otherwise("other").alias("priority_group"),
        F.initcap(F.trim(F.col("channel"))).alias("channel_clean"),
    )
)

with_details = (
    base.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.priority_group",
        "s.channel_clean",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

sequenced = (
    with_details
    .select(
        "*",
        F.nth_value("opened_at", 2).over(w_agent_id_priority_by_request).alias("second_opened_at"),
        F.size(F.collect_set("priority").over(w_agent_hired_on_month_priorit)).alias("distinct_priority_in_agent_hired_on_month_priori"),
        F.sum("requester_id").over(w_agent_id_priority_by_request_2).alias("requester_id_sum_3_prior"),
        F.rank().over(w_agent_id_priority_by_request_3).alias("requester_id_rank"),
        F.first("requester_id").over(w_agent_id_priority_by_request).alias("first_requester_id"),
        F.first("agent_hired_on", ignorenulls=True).over(w_agent_id_by_closed_at_runnin).alias("first_known_agent_hired_on"),
        F.rank().over(w_all_by_agent_hired_on).alias("agent_hired_on_rank"),
        F.rank().over(w_all_by_agent_hired_on_ties).alias("agent_hired_on_rank_2"),
        F.count(F.lit(1)).over(w_agent_id_priority).alias("rows_in_agent_id_priority"),
    )
)

scored = (
    sequenced
    .select(
        "*",
        F.abs(F.col("requester_id_sum_3_prior") - 27).alias("requester_id_sum_3_prior_distance"),
        F.date_trunc("day", F.col("closed_at")).alias("closed_at_day"),
        F.length(F.trim(F.col("agent_team"))).alias("agent_team_len"),
    )
)

windowed = (
    scored
    .select(
        "*",
        F.datediff(F.col("first_known_agent_hired_on"), F.lag("first_known_agent_hired_on").over(w_agent_team_len_opened_at_mon)).alias("days_since_prev_first_known_agent_hired_on"),
        F.size(F.collect_set("priority").over(w_distinct_priority_in_agent_h)).alias("distinct_priority_in_distinct_priority_in_agent_"),
        F.min("rows_in_agent_id_priority").over(w_distinct_priority_in_agent_h).alias("distinct_priority_in_agent_hired_on_month_priori_2"),
    )
)

per_group = (
    windowed
    .groupBy(
        F.year("opened_at").alias("opened_at_year"),
        "distinct_priority_in_distinct_priority_in_agent_",
    )
    .agg(
        F.round(F.stddev("requester_id_rank"), 2).alias("stddev_requester_id_rank"),
        F.array_sort(F.collect_set("agent_team")).alias("agent_team_list"),
        F.min("requester_id_sum_3_prior").alias("min_requester_id_sum_3_prior"),
        F.avg("distinct_priority_in_agent_hired_on_month_priori_2").alias("avg_distinct_priority_in_agent_hired_on_month_pr"),
    )
    .filter(F.col("avg_distinct_priority_in_agent_hired_on_month_pr") > 71)
    .drop("avg_distinct_priority_in_agent_hired_on_month_pr")
)

result = (
    per_group
    .select(
        "opened_at_year",
        "distinct_priority_in_distinct_priority_in_agent_",
        "agent_team_list",
    )
    .orderBy("distinct_priority_in_distinct_priority_in_agent_")
)
