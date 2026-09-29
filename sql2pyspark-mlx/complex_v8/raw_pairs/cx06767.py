from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_closed_at_month = Window.partitionBy("agent_id", F.date_trunc("month", "closed_at"))
w_priority_len_priority_by_pri = Window.partitionBy("priority_len", "priority").orderBy(F.col("priority_len").desc(), "id")
w_agent_id_opened_at_month_by = Window.partitionBy("agent_id", F.month("opened_at")).orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_by_csat = Window.partitionBy("priority").orderBy(F.col("csat").desc(), "id")
w_priority_len = Window.partitionBy("priority_len")

prepared_tickets = (
    df_tickets
    .filter((F.hour(F.col("opened_at")).between(9, 11)) & (F.col("requester_id") > 7))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

with_trends = (
    prepared_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "csat",
        "tags_text",
        "priority_len",
        F.sum("csat").over(w_agent_id_closed_at_month).alias("agent_id_closed_at_month_sum_csat"),
        F.lag("csat").over(w_priority_len_priority_by_pri).alias("prev_csat"),
        F.nth_value("csat", 2).over(w_agent_id_opened_at_month_by).alias("second_csat"),
        F.lead("requester_id").over(w_priority_by_csat).alias("next_requester_id"),
        F.last("opened_at").over(w_agent_id_opened_at_month_by).alias("last_opened_at"),
        F.round(F.percent_rank().over(w_priority_by_csat), 3).alias("csat_percent_rank"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id_closed_at_month), F.lit(0)), 4).alias("requester_id_share_of_agent_id_closed_at_month"),
        F.ntile(10).over(w_priority_len_priority_by_pri).alias("priority_len_decile"),
        F.size(F.collect_set("priority").over(w_priority_len)).alias("distinct_priority_in_priority_len"),
        F.count(F.lit(1)).over(w_agent_id_closed_at_month).alias("rows_in_agent_id_closed_at_month"),
        F.size(F.collect_set("priority").over(w_agent_id_closed_at_month)).alias("distinct_priority_in_agent_id_closed_at_month"),
        F.max("priority_len").over(w_priority_len).alias("priority_len_max_priority_len"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "csat",
        "tags_text",
        "priority_len",
        "agent_id_closed_at_month_sum_csat",
        "prev_csat",
        "next_requester_id",
        "last_opened_at",
        "csat_percent_rank",
        "priority_len_decile",
        "rows_in_agent_id_closed_at_month",
    )
    .orderBy(
        F.col("id").desc_nulls_first(),
        F.col("next_requester_id").asc_nulls_last(),
    )
)
