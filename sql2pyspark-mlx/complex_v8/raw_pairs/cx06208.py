from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_closed_at_year_by_csat = Window.partitionBy(F.year("closed_at")).orderBy("csat", "id")
w_agent_id_priority_by_rows_in = Window.partitionBy("agent_id", "priority").orderBy("rows_in_agent_id_csat", "id")
w_agent_id_priority_by_rows_in_2 = Window.partitionBy("agent_id", "priority").orderBy("rows_in_agent_id_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_closed_at_month_priority = Window.partitionBy(F.date_trunc("month", "closed_at"), "priority")
w_agent_id_priority_by_rows_in_3 = Window.partitionBy("agent_id", "priority").orderBy("rows_in_agent_id_csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_csat_percent_rank = Window.orderBy("csat_percent_rank", "id")
w_priority_group_first_closed = Window.partitionBy("priority_group", F.year("first_closed_at"))
w_opened_at_month_agent_id_by = Window.partitionBy("opened_at_month", "agent_id").orderBy("csat_plus_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_tickets
    .filter(F.col("requester_id") > 4)
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
        F.substring(F.trim(F.col("priority")), 1, 3).alias("priority_prefix"),
        F.month(F.col("opened_at")).alias("opened_at_month"),
    )
)

windowed = (
    base
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        "csat_plus_requester_id",
        "priority_prefix",
        "opened_at_month",
        F.count(F.lit(1)).over(w_agent_id_csat).alias("rows_in_agent_id_csat"),
        F.round(F.percent_rank().over(w_closed_at_year_by_csat), 3).alias("csat_percent_rank"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.row_number().over(w_agent_id_priority_by_rows_in).alias("rn"),
        F.first("csat_plus_requester_id", ignorenulls=True).over(w_agent_id_priority_by_rows_in_2).alias("first_known_csat_plus_requester_id"),
        F.max("csat_percent_rank").over(w_closed_at_month_priority).alias("closed_at_month_priority_max_csat_percent_rank"),
        F.first("closed_at").over(w_agent_id_priority_by_rows_in_3).alias("first_closed_at"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.when(F.col("priority").isin("P3", "P4"), "group_a").when(F.col("priority") == "P2", "group_b").otherwise("other").alias("priority_group"),
        F.round(F.col("csat_percent_rank") / F.nullif(F.col("rn"), F.lit(0)), 3).alias("csat_percent_rank_per_rn"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("opened_at").alias("first_ticket_opened_at"),
        F.sum("csat").alias("total_ticket_csat"),
    )
)

profiled = (
    scored.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.first_ticket_opened_at", "t.total_ticket_csat")
)

metrics = (
    profiled
    .select(
        "*",
        F.row_number().over(w_all_by_csat_percent_rank).alias("seq_no"),
        F.min("rn").over(w_priority_group_first_closed).alias("priority_group_first_closed_at_year_min_rn"),
        F.size(F.collect_set("channel").over(w_priority_group_first_closed)).alias("distinct_channel_in_priority_group_first_closed_"),
        F.nth_value("opened_at_month", 2).over(w_opened_at_month_agent_id_by).alias("second_opened_at_month"),
        F.size(F.collect_set("agent_id").over(w_priority_group_first_closed)).alias("distinct_agent_id_in_priority_group_first_closed"),
    )
)

result = (
    metrics
    .select(
        "id",
        "csat_percent_rank",
        "closed_at_month_priority_max_csat_percent_rank",
    )
    .unpivot(
        ids=["id"],
        values=["csat_percent_rank", "closed_at_month_priority_max_csat_percent_rank"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
