from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_to_date = Window.partitionBy(F.to_date("closed_at"))
w_closed_at_month_by_closed_at = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy(F.col("closed_at").asc_nulls_last(), "id")
w_agent_id_csat_by_requester_i = Window.partitionBy("agent_id", "csat").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_closed_at_month_by_csat_runn = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_csat_by_requester_i_2 = Window.partitionBy("agent_id", "csat").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat = Window.partitionBy("csat")
w_all_by_requester_id_distance = Window.orderBy("requester_id_distance", "id")
w_first_opened_at_to_date_by_f = Window.partitionBy(F.to_date("first_opened_at")).orderBy("first_opened_at", "id").rowsBetween(-1, Window.currentRow)
w_csat_closed_at_decile_by_csa = Window.partitionBy("csat", "closed_at_decile").orderBy(F.col("csat").desc(), "id")
w_csat_closed_at_decile_by_csa_2 = Window.partitionBy("csat", "closed_at_decile").orderBy(F.col("csat").desc())

cleaned_tickets = (
    df_tickets
    .filter(F.month(F.col("opened_at")).isin(1, 2))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.abs(F.col("requester_id") - 13).alias("requester_id_distance"),
    )
)

sequenced = (
    cleaned_tickets
    .select(
        "*",
        F.round(F.avg("requester_id_distance").over(w_closed_at_to_date), 2).alias("closed_at_to_date_avg_requester_id_distance"),
        F.row_number().over(w_closed_at_month_by_closed_at).alias("seq_no"),
        F.sum("requester_id_distance").over(w_agent_id_csat_by_requester_i).alias("running_requester_id_distance"),
        F.first("opened_at", ignorenulls=True).over(w_closed_at_month_by_csat_runn).alias("first_known_opened_at"),
        F.max("requester_id_distance").over(w_closed_at_to_date).alias("closed_at_to_date_max_requester_id_distance"),
        F.last("closed_at", ignorenulls=True).over(w_agent_id_csat_by_requester_i).alias("last_known_closed_at"),
        F.ntile(10).over(w_closed_at_month_by_closed_at).alias("closed_at_decile"),
        F.first("opened_at").over(w_agent_id_csat_by_requester_i_2).alias("first_opened_at"),
        F.round(F.col("requester_id_distance") - F.avg("requester_id_distance").over(w_closed_at_to_date), 2).alias("requester_id_distance_vs_closed_at_to_date_avg"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_closed_at_to_date), 2).alias("requester_id_vs_closed_at_to_date_avg"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_closed_at_to_date), F.lit(0)), 4).alias("requester_id_share_of_closed_at_to_date"),
        F.count(F.lit(1)).over(w_closed_at_to_date).alias("rows_in_closed_at_to_date"),
        F.round(F.avg("requester_id").over(w_closed_at_to_date), 2).alias("closed_at_to_date_avg_requester_id"),
    )
)

with_trends = (
    sequenced
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "requester_id_distance",
        "seq_no",
        "running_requester_id_distance",
        "requester_id_distance_vs_closed_at_to_date_avg",
        "requester_id_share_of_closed_at_to_date",
        "rows_in_closed_at_to_date",
        "closed_at_to_date_avg_requester_id",
        F.sum("rows_in_closed_at_to_date").over(w_csat).alias("csat_sum_rows_in_closed_at_to_date"),
        F.round((F.unix_timestamp("first_opened_at") - F.unix_timestamp(F.lag("first_opened_at").over(w_all_by_requester_id_distance))) / 60, 1).alias("minutes_since_prev_first_opened_at"),
        F.size(F.collect_set("agent_id").over(w_csat)).alias("distinct_agent_id_in_csat"),
        F.max("seq_no").over(w_first_opened_at_to_date_by_f).alias("seq_no_max_2"),
        F.lead("requester_id", 1, 0).over(w_csat_closed_at_decile_by_csa).alias("next_requester_id"),
        F.dense_rank().over(w_csat_closed_at_decile_by_csa_2).alias("csat_dense_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "requester_id_distance",
        "seq_no",
        "running_requester_id_distance",
        "requester_id_distance_vs_closed_at_to_date_avg",
        "requester_id_share_of_closed_at_to_date",
        "rows_in_closed_at_to_date",
        "csat_dense_rank",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
    )
    .orderBy(F.col("opened_at").desc(), "csat", "id")
    .limit(20)
)
