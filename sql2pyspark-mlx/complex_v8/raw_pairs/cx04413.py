from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_len_cnt_by_requeste = Window.partitionBy("priority_len", "cnt").orderBy(F.col("requester_id_to_priority_len_ratio").desc(), "priority_len", "priority", "opened_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_priority_len = Window.partitionBy("priority", "priority_len")
w_priority_len_cnt_by_requeste_2 = Window.partitionBy("priority_len", "cnt").orderBy(F.col("requester_id_to_priority_len_ratio").desc(), "priority_len", "priority", "opened_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_second_priority_len = Window.partitionBy("second_priority_len")

recent = (
    df_tickets
    .filter((F.col("requester_id") > 7) & (F.col("csat") >= 3))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

totals = (
    recent
    .filter(F.hour(F.col("closed_at")).between(10, 14))
    .groupBy(
        "priority_len",
        "priority",
        F.date_trunc("month", "opened_at").alias("opened_at_month"),
    )
    .agg(
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.round(F.sum("requester_id") / F.nullif(F.sum("priority_len"), F.lit(0)), 3).alias("requester_id_to_priority_len_ratio"),
        F.count("*").alias("cnt"),
        F.min("requester_id").alias("min_requester_id"),
    )
)

qualified = (
    totals
    .crossJoin(totals.agg(F.avg("requester_id_to_priority_len_ratio").alias("overall_avg_requester_id_to_priority_len_ratio")))
    .filter(F.col("requester_id_to_priority_len_ratio") > F.col("overall_avg_requester_id_to_priority_len_ratio"))
    .drop("overall_avg_requester_id_to_priority_len_ratio")
)

metrics = (
    qualified
    .select(
        "priority_len",
        "priority",
        "opened_at_month",
        "channel_list",
        "requester_id_to_priority_len_ratio",
        "cnt",
        "min_requester_id",
        F.nth_value("priority_len", 2).over(w_priority_len_cnt_by_requeste).alias("second_priority_len"),
        F.count(F.lit(1)).over(w_priority_priority_len).alias("rows_in_priority_priority_len"),
        F.first("cnt", ignorenulls=True).over(w_priority_len_cnt_by_requeste_2).alias("first_known_cnt"),
    )
)

labeled = (
    metrics
    .select(
        "priority_len",
        "priority",
        "opened_at_month",
        "channel_list",
        "min_requester_id",
        "second_priority_len",
        "first_known_cnt",
        F.dayofweek(F.col("opened_at_month")).alias("opened_at_month_dayofweek"),
    )
)

metrics_2 = (
    labeled
    .select(
        "priority_len",
        "priority",
        "opened_at_month",
        "channel_list",
        "min_requester_id",
        "second_priority_len",
        "first_known_cnt",
        "opened_at_month_dayofweek",
        F.round(F.col("min_requester_id") / F.nullif(F.sum("min_requester_id").over(w_second_priority_len), F.lit(0)), 4).alias("min_requester_id_share_of_second_priority_len"),
        F.round(F.avg("opened_at_month_dayofweek").over(w_second_priority_len), 2).alias("second_priority_len_avg_opened_at_month_dayofwee"),
        F.size(F.collect_set("priority").over(w_second_priority_len)).alias("distinct_priority_in_second_priority_len"),
        F.sum("first_known_cnt").over(w_second_priority_len).alias("second_priority_len_sum_first_known_cnt"),
    )
)

result = (
    metrics_2
    .select(
        "priority_len",
        "priority",
        F.col("opened_at_month_dayofweek").alias("m_opened_at_month_dayofweek"),
        F.col("second_priority_len").alias("m_second_priority_len"),
        F.col("min_requester_id").alias("min_requester_id_v"),
        F.col("distinct_priority_in_second_priority_len").alias("m_distinct_priority_in_second_priority_len"),
    )
    .unpivot(
        ids=["priority_len", "priority"],
        values=["m_opened_at_month_dayofweek", "m_second_priority_len", "min_requester_id_v", "m_distinct_priority_in_second_priority_len"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
