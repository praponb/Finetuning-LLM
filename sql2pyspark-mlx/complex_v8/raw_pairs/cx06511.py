from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority")
w_opened_at_month_opened_at_qu = Window.partitionBy(F.month("opened_at"), F.month("opened_at_quarter")).orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_opened_at_month_csat_by_requ = Window.partitionBy(F.month("opened_at"), "csat").orderBy(F.col("requester_id").desc(), "id")
w_priority_by_agent_hired_on_r = Window.partitionBy("priority").orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_by_agent_hired_on = Window.partitionBy("priority").orderBy("agent_hired_on", "id")

base_tickets = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        F.when(F.col("priority").isin("P4", "P2"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
        F.date_trunc("quarter", F.col("opened_at")).alias("opened_at_quarter"),
    )
)

joined_agent = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select("s.*", F.col("su.hired_on").alias("agent_hired_on"), "su.agent_name")
)

metrics = (
    joined_agent
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
        "agent_name",
        F.min("csat").over(w_priority).alias("priority_min_csat"),
        F.ntile(10).over(w_opened_at_month_opened_at_qu).alias("opened_at_decile"),
        F.max("requester_id").over(w_priority).alias("priority_max_requester_id"),
        F.rank().over(w_opened_at_month_csat_by_requ).alias("requester_id_rank"),
        F.count(F.lit(1)).over(w_priority).alias("rows_in_priority"),
        F.sum("csat").over(w_priority_by_agent_hired_on_r).alias("running_csat"),
        F.size(F.collect_set("priority_group").over(w_priority)).alias("distinct_priority_group_in_priority"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_priority), F.lit(0)), 4).alias("csat_share_of_priority"),
        F.ntile(4).over(w_opened_at_month_csat_by_requ).alias("requester_id_quartile"),
        F.round(F.percent_rank().over(w_priority_by_agent_hired_on), 3).alias("agent_hired_on_percent_rank"),
        F.round(F.avg("requester_id").over(w_priority), 2).alias("priority_avg_requester_id"),
        F.round(F.percent_rank().over(w_opened_at_month_csat_by_requ), 3).alias("requester_id_percent_rank"),
        F.min("requester_id").over(w_priority).alias("priority_min_requester_id"),
        F.round(F.cume_dist().over(w_opened_at_month_csat_by_requ), 3).alias("requester_id_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
        "agent_name",
        "priority_min_csat",
        "opened_at_decile",
        "priority_max_requester_id",
        "requester_id_rank",
        "rows_in_priority",
        "distinct_priority_group_in_priority",
        "requester_id_quartile",
        "agent_hired_on_percent_rank",
        "requester_id_percent_rank",
        "requester_id_cume_dist",
        F.dayofweek(F.col("opened_at")).alias("opened_at_dayofweek"),
    )
    .orderBy("agent_id")
)
