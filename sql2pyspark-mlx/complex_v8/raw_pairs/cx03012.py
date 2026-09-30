from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy("event_at")
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id")
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last())
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc_nulls_first(), "id")
w_event_at_month_by_event_at = Window.partitionBy(F.month("event_at")).orderBy("event_at", "id")
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy("event_at", "id")
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc())
w_event_at_rank = Window.partitionBy("event_at_rank")
w_prev_event_at_month_by_seq_n = Window.partitionBy(F.date_trunc("month", "prev_event_at")).orderBy(F.col("seq_no").desc(), "id").rowsBetween(-3, Window.currentRow)
w_prev_event_at_2_year_next_ev = Window.partitionBy(F.year("prev_event_at_2"), F.date_trunc("month", "next_event_at")).orderBy("next2_event_at", "id")
w_prev_event_at_2_year_next_ev_2 = Window.partitionBy(F.year("prev_event_at_2"), F.date_trunc("month", "next_event_at")).orderBy("next2_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_cnt_by_cnt = Window.partitionBy("cnt").orderBy(F.col("cnt").desc(), "prev2_event_at_month", "prev_event_at_year")
w_cnt_n_actor_in_system_custom = Window.partitionBy("cnt", "n_actor_in_system_customer")

filtered_events = (
    df_ticket_events
    .filter((F.col("actor").isNotNull()) & (F.col("ticket_id") <= 10))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

metrics = (
    filtered_events
    .select(
        "*",
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at"),
        F.rank().over(w_actor_ticket_id_by_event_at).alias("event_at_rank"),
        F.ntile(3).over(w_actor_ticket_id_by_event_at_2).alias("event_at_tercile"),
        F.round(F.cume_dist().over(w_actor_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.ntile(5).over(w_event_type_actor_by_event_at).alias("event_at_quintile"),
        F.lag("event_at", 2).over(w_ticket_id_by_event_at).alias("prev2_event_at"),
        F.lag("event_at").over(w_event_type_actor_by_event_at).alias("prev_event_at"),
        F.row_number().over(w_event_at_month_by_event_at).alias("seq_no"),
        F.lag("event_at").over(w_event_type_ticket_id_by_even).alias("prev_event_at_2"),
        F.lead("event_at").over(w_ticket_id_by_event_at).alias("next_event_at"),
        F.lead("event_at", 2).over(w_event_type_actor_by_event_at).alias("next2_event_at"),
        F.round(F.cume_dist().over(w_event_type_actor_by_event_at_2), 3).alias("event_at_cume_dist_2"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.sum("seq_no").over(w_event_at_rank).alias("event_at_rank_sum_seq_no"),
        F.round(F.avg("seq_no").over(w_prev_event_at_month_by_seq_n), 2).alias("seq_no_avg_4"),
        F.round((F.unix_timestamp("prev2_event_at") - F.unix_timestamp(F.lag("prev2_event_at").over(w_prev_event_at_2_year_next_ev))) / 60, 1).alias("minutes_since_prev_prev2_event_at"),
        F.round(F.col("event_at_tercile") / F.nullif(F.sum("event_at_tercile").over(w_event_at_rank), F.lit(0)), 4).alias("event_at_tercile_share_of_event_at_rank"),
        F.last("event_at").over(w_prev_event_at_2_year_next_ev_2).alias("last_event_at"),
    )
)

per_group = (
    with_trends
    .groupBy(
        F.date_trunc("month", "prev2_event_at").alias("prev2_event_at_month"),
        F.year("prev_event_at").alias("prev_event_at_year"),
    )
    .agg(
        F.percentile_approx("event_at_tercile_share_of_event_at_rank", 0.5).alias("median_event_at_tercile_share_of_event_at_rank"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("actor") != "customer", F.col("seq_no_avg_4"))).alias("seq_no_avg_4_actor_customer"),
        F.count(F.when(F.col("actor").isin("system", "customer"), 1)).alias("n_actor_in_system_customer"),
        F.round(100 * F.count(F.when(F.col("actor").isNotNull(), 1)) / F.count("*"), 1).alias("pct_actor_is_not_null"),
    )
)

with_windows = (
    per_group
    .select(
        "*",
        (F.col("prev_event_at_year") - F.lag("prev_event_at_year").over(w_cnt_by_cnt)).alias("prev_event_at_year_change"),
        F.count(F.lit(1)).over(w_cnt_n_actor_in_system_custom).alias("rows_in_cnt_n_actor_in_system_customer"),
        F.sum("seq_no_avg_4_actor_customer").over(w_cnt_n_actor_in_system_custom).alias("cnt_n_actor_in_system_customer_sum_seq_no_avg_4_"),
    )
)

result = (
    with_windows
    .select("prev2_event_at_month")
    .distinct()
)
