from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_by_event_at = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at_runnin = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_actor_group_event_at_month_b = Window.partitionBy("actor_group", F.date_trunc("month", "event_at")).orderBy("event_at", "id")
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy("event_at")
w_event_at_month_by_event_at_t = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy(F.col("event_at").desc())
w_event_at_month_by_event_at_f = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_group_event_at_month_b_2 = Window.partitionBy("actor_group", F.date_trunc("month", "event_at")).orderBy("event_at")
w_all_by_event_at_seq_ties = Window.orderBy("event_at_seq")
w_all_by_event_at_seq_moving_3 = Window.orderBy("event_at_seq", "id").rowsBetween(-3, Window.currentRow)
w_event_at_rank_2_last_event_a = Window.partitionBy("event_at_rank_2", F.date_trunc("month", "last_event_at"))
w_all_by_event_at_seq = Window.orderBy("event_at_seq", "id")

base_events = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 51)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("system", "agent"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
    )
)

ranked = (
    base_events
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        F.lead("event_at", 2).over(w_event_at_month_by_event_at).alias("next2_event_at"),
        F.first("event_at", ignorenulls=True).over(w_ticket_id_by_event_at_runnin).alias("first_known_event_at"),
        F.row_number().over(w_ticket_id_by_event_at).alias("event_at_seq"),
        F.lag("event_at").over(w_ticket_id_by_event_at).alias("prev_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_month_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_group_event_at_month_b))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.rank().over(w_event_at_month_by_event_at_t).alias("event_at_rank"),
        F.nth_value("event_at", 2).over(w_event_at_month_by_event_at_f).alias("second_event_at"),
        F.rank().over(w_ticket_id_by_event_at_ties).alias("event_at_rank_2"),
        F.last("event_at").over(w_event_at_month_by_event_at_f).alias("last_event_at"),
        F.dense_rank().over(w_actor_group_event_at_month_b_2).alias("event_at_dense_rank"),
        F.rank().over(w_actor_group_event_at_month_b).alias("event_at_rank_3"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at), 3).alias("event_at_cume_dist"),
    )
)

first_rows = (
    ranked
    .filter((F.col("event_at_seq") == 1) & (F.hour(F.col("next2_event_at")).between(10, 16)))
)

ranked_2 = (
    first_rows
    .select(
        "id",
        "ticket_id",
        "event_type",
        "next2_event_at",
        "event_at_seq",
        "minutes_since_prev_event_at",
        "minutes_since_prev_event_at_2",
        "event_at_percent_rank",
        "second_event_at",
        "event_at_rank_2",
        F.dense_rank().over(w_all_by_event_at_seq_ties).alias("event_at_seq_dense_rank"),
        F.sum("minutes_since_prev_event_at").over(w_all_by_event_at_seq_moving_3).alias("minutes_since_prev_event_at_sum_4"),
        F.size(F.collect_set("actor").over(w_event_at_rank_2_last_event_a)).alias("distinct_actor_in_event_at_rank_2_last_event_at_"),
        F.round(F.col("event_at_rank") - F.avg("event_at_rank").over(w_event_at_rank_2_last_event_a), 2).alias("event_at_rank_vs_event_at_rank_2_last_event_at_m"),
        F.row_number().over(w_all_by_event_at_seq).alias("rn"),
        F.count(F.lit(1)).over(w_event_at_rank_2_last_event_a).alias("rows_in_event_at_rank_2_last_event_at_month"),
    )
)

result = (
    ranked_2
    .filter(F.col("distinct_actor_in_event_at_rank_2_last_event_at_") < 6)
    .select(
        "id",
        "ticket_id",
        "next2_event_at",
        "event_at_seq",
        "minutes_since_prev_event_at",
        "event_at_percent_rank",
        "second_event_at",
        "event_at_rank_2",
        "event_at_seq_dense_rank",
        "minutes_since_prev_event_at_sum_4",
        "distinct_actor_in_event_at_rank_2_last_event_at_",
        "event_at_rank_vs_event_at_rank_2_last_event_at_m",
        "rn",
    )
    .orderBy("minutes_since_prev_event_at_sum_4", "id")
    .limit(5)
)
