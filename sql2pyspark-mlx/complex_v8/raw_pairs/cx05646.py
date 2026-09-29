from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_year_channel_len = Window.partitionBy(F.year("closed_at"), "channel_len")
w_all_by_channel_len_ties = Window.orderBy("channel_len")
w_all_by_channel_len = Window.orderBy("channel_len", "id")
w_agent_id = Window.partitionBy("agent_id")
w_opened_at_month = Window.partitionBy(F.date_trunc("month", "opened_at"))
w_closed_at_to_date_by_closed = Window.partitionBy(F.to_date("closed_at")).orderBy(F.col("closed_at").desc(), "id")
w_all_by_closed_at_prior_3 = Window.orderBy(F.col("closed_at").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_row_count_channel_len_quinti = Window.partitionBy("row_count", "channel_len_quintile").orderBy(F.col("total_rows_in_opened_at_month").desc(), "channel_len_quintile", "closed_at_dense_rank", "channel_len").rowsBetween(-6, Window.currentRow)
w_channel_len_quintile_by_chan = Window.partitionBy("channel_len_quintile").orderBy(F.col("channel_len_vs_closed_at_year_channel_len_avg_to").desc(), "channel_len_quintile", "closed_at_dense_rank", "channel_len")

recent_tickets = (
    df_tickets
    .filter(~F.col("channel").isin("email", "web"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        F.date_format(F.col("closed_at"), "yyyy-MM").alias("closed_at_label"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

with_windows = (
    recent_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        "channel_len",
        F.size(F.collect_set("agent_id").over(w_closed_at_year_channel_len)).alias("distinct_agent_id_in_closed_at_year_channel_len"),
        F.dense_rank().over(w_all_by_channel_len_ties).alias("channel_len_dense_rank"),
        F.ntile(5).over(w_all_by_channel_len).alias("channel_len_quintile"),
        F.count(F.lit(1)).over(w_closed_at_year_channel_len).alias("rows_in_closed_at_year_channel_len"),
        F.min("requester_id").over(w_closed_at_year_channel_len).alias("closed_at_year_channel_len_min_requester_id"),
        F.round(F.col("channel_len") - F.avg("channel_len").over(w_agent_id), 2).alias("channel_len_vs_agent_id_avg"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_id), 2).alias("requester_id_vs_agent_id_avg"),
        F.min("channel_len").over(w_agent_id).alias("agent_id_min_channel_len"),
        F.round(F.col("channel_len") - F.avg("channel_len").over(w_closed_at_year_channel_len), 2).alias("channel_len_vs_closed_at_year_channel_len_avg"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_opened_at_month), F.lit(0)), 4).alias("requester_id_share_of_opened_at_month"),
        F.dense_rank().over(w_closed_at_to_date_by_closed).alias("closed_at_dense_rank"),
        F.max("requester_id").over(w_all_by_closed_at_prior_3).alias("requester_id_max_3_prior"),
        F.sum("requester_id").over(w_opened_at_month).alias("opened_at_month_sum_requester_id"),
        F.count(F.lit(1)).over(w_opened_at_month).alias("rows_in_opened_at_month"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_closed_at_year_channel_len), 2).alias("requester_id_vs_closed_at_year_channel_len_avg"),
    )
)

totals = (
    with_windows
    .groupBy("channel_len_quintile", "closed_at_dense_rank", "channel_len")
    .agg(
        F.sum("rows_in_opened_at_month").alias("total_rows_in_opened_at_month"),
        F.round(F.sum("channel_len_vs_closed_at_year_channel_len_avg") / F.nullif(F.sum("requester_id_max_3_prior"), F.lit(0)), 3).alias("channel_len_vs_closed_at_year_channel_len_avg_to"),
        F.min("opened_at").alias("first_opened_at"),
        F.count("*").alias("row_count"),
        F.max("opened_at").alias("last_opened_at"),
        F.min("closed_at").alias("first_closed_at"),
    )
)

with_trends = (
    totals
    .select(
        "*",
        F.round(F.avg("row_count").over(w_row_count_channel_len_quinti), 2).alias("row_count_avg_7"),
        F.lead("closed_at_dense_rank", 3).over(w_channel_len_quintile_by_chan).alias("next3_closed_at_dense_rank"),
    )
)

result = (
    with_trends
    .select(
        "channel_len_quintile",
        "closed_at_dense_rank",
        "channel_len",
        "total_rows_in_opened_at_month",
        "first_opened_at",
        "row_count",
        "row_count_avg_7",
        "next3_closed_at_dense_rank",
        F.date_trunc("month", F.col("first_closed_at")).alias("first_closed_at_month"),
    )
    .orderBy(
        F.col("total_rows_in_opened_at_month").desc(),
        F.col("closed_at_dense_rank").asc_nulls_last(),
    )
)
