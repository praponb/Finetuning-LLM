from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_month_csat_by_clos = Window.partitionBy(F.month("opened_at"), "csat").orderBy("closed_at", "id")
w_channel_by_closed_at_running = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_by_csat_minus_reque = Window.partitionBy("priority").orderBy("csat_minus_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_channel_by_closed_at = Window.partitionBy("csat", "channel").orderBy("closed_at", "id")
w_channel_by_closed_at = Window.partitionBy("channel").orderBy("closed_at", "id")
w_opened_at_year = Window.partitionBy(F.year("opened_at"))
w_csat_by_csat = Window.partitionBy("csat").orderBy("csat", "id")
w_closed_at_seq_closed_at_rank = Window.partitionBy("closed_at_seq", "closed_at_rank")

base_tickets = (
    df_tickets
    .filter((~F.col("priority").isin("P4", "P3")) & (F.col("channel") == "web"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        (F.col("csat") - F.col("requester_id")).alias("csat_minus_requester_id"),
    )
)

ranked = (
    base_tickets
    .select(
        "*",
        F.round(F.percent_rank().over(w_opened_at_month_csat_by_clos), 3).alias("closed_at_percent_rank"),
        F.max("csat").over(w_channel_by_closed_at_running).alias("max_csat_so_far"),
        F.min("csat").over(w_priority_by_csat_minus_reque).alias("min_csat_so_far"),
        F.row_number().over(w_csat_channel_by_closed_at).alias("closed_at_seq"),
        F.ntile(4).over(w_channel_by_closed_at).alias("closed_at_quartile"),
        F.count(F.lit(1)).over(w_opened_at_year).alias("rows_in_opened_at_year"),
        F.sum("csat_minus_requester_id").over(w_opened_at_year).alias("opened_at_year_sum_csat_minus_requester_id"),
        F.rank().over(w_csat_channel_by_closed_at).alias("closed_at_rank"),
        (F.col("csat") - F.lag("csat").over(w_csat_by_csat)).alias("csat_change"),
    )
)

ranked_2 = (
    ranked
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat_minus_requester_id",
        "closed_at_percent_rank",
        "closed_at_seq",
        "closed_at_quartile",
        "rows_in_opened_at_year",
        "closed_at_rank",
        "csat_change",
        F.round(F.col("closed_at_rank") - F.avg("closed_at_rank").over(w_closed_at_seq_closed_at_rank), 2).alias("closed_at_rank_vs_closed_at_seq_closed_at_rank_a"),
        F.count(F.lit(1)).over(w_closed_at_seq_closed_at_rank).alias("rows_in_closed_at_seq_closed_at_rank"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "csat_minus_requester_id",
        "closed_at_percent_rank",
        "closed_at_seq",
        "closed_at_quartile",
        "rows_in_opened_at_year",
        "closed_at_rank",
        "csat_change",
        "rows_in_closed_at_seq_closed_at_rank",
        F.coalesce(F.col("closed_at_quartile"), F.lit(0)).alias("closed_at_quartile_filled"),
        F.round(F.col("closed_at_percent_rank") * 0.9, 2).alias("closed_at_percent_rank_adj"),
    )
    .orderBy(F.col("opened_at").desc_nulls_first(), F.col("id").asc_nulls_last())
)
