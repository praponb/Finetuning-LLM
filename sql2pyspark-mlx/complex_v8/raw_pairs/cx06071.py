from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at_2 = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy(F.col("event_at").desc())
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_event_at_month_by_eve = Window.partitionBy(F.date_trunc("month", "second_event_at")).orderBy("event_at_quartile", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_event_at_month_by_eve_2 = Window.partitionBy(F.date_trunc("month", "second_event_at")).orderBy("event_at_quartile", "id").rowsBetween(-2, -1)
w_all_by_rn = Window.orderBy("rn", "id")
w_all_by_rn_ties = Window.orderBy("rn")
w_prev_event_at_to_date_seq_no = Window.partitionBy(F.to_date("prev_event_at"), "seq_no")
w_event_at_rank_2_event_at_ran = Window.partitionBy("event_at_rank_2", "event_at_rank").orderBy(F.col("event_at_percent_rank_2").desc())
w_min_rn_rn_dense_rank_by_n_ev = Window.partitionBy("min_rn", "rn_dense_rank").orderBy(F.col("n_event_type_replied").desc())
w_min_rn_rn_dense_rank_by_n_ev_2 = Window.partitionBy("min_rn", "rn_dense_rank").orderBy(F.col("n_event_type_replied").desc(), "rn_dense_rank")

prepared_events = (
    df_ticket_events
    .filter(~F.col("actor").isin("customer", "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)

ranked = (
    prepared_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor_ref",
        F.rank().over(w_ticket_id_actor_by_event_at).alias("event_at_rank"),
        F.round(F.percent_rank().over(w_event_type_by_event_at), 3).alias("event_at_percent_rank"),
        F.first("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("first_known_event_at"),
        F.rank().over(w_event_type_by_event_at_2).alias("event_at_rank_2"),
        F.first("event_at").over(w_event_type_by_event_at_full).alias("first_event_at"),
        F.row_number().over(w_ticket_id_actor_by_event_at).alias("rn"),
        F.round(F.percent_rank().over(w_event_type_by_event_at_ties), 3).alias("event_at_percent_rank_2"),
        F.row_number().over(w_event_type_by_event_at).alias("seq_no"),
        F.lag("event_at").over(w_ticket_id_actor_by_event_at).alias("prev_event_at"),
        F.ntile(4).over(w_ticket_id_actor_by_event_at).alias("event_at_quartile"),
        F.dense_rank().over(w_event_type_by_event_at_2).alias("event_at_dense_rank"),
        F.nth_value("event_at", 2).over(w_actor_event_type_by_event_at).alias("second_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_by_event_at_2))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

top_rows = ranked.filter(F.col("event_at_dense_rank") == 1)

metrics = (
    top_rows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor_ref",
        "event_at_rank",
        "event_at_rank_2",
        "first_event_at",
        "rn",
        "prev_event_at",
        "event_at_quartile",
        "event_at_dense_rank",
        "minutes_since_prev_event_at",
        F.last("seq_no").over(w_second_event_at_month_by_eve).alias("last_seq_no"),
        F.max("minutes_since_prev_event_at").over(w_second_event_at_month_by_eve_2).alias("minutes_since_prev_event_at_max_2_prior"),
        F.row_number().over(w_all_by_rn).alias("rn_seq"),
        F.dense_rank().over(w_all_by_rn_ties).alias("rn_dense_rank"),
        F.round(F.avg("event_at_rank").over(w_prev_event_at_to_date_seq_no), 2).alias("prev_event_at_to_date_seq_no_avg_event_at_rank"),
        F.round(F.percent_rank().over(w_event_at_rank_2_event_at_ran), 3).alias("event_at_percent_rank_2_percent_rank"),
    )
)

summary = (
    metrics
    .filter(F.col("rn_seq") < 9)
    .groupBy("rn_dense_rank")
    .agg(
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.sum(F.when(F.col("event_type") == "replied", 1).otherwise(0)).alias("n_event_type_replied"),
        F.min("rn").alias("min_rn"),
        F.max("event_at").alias("last_event_at"),
        F.max("event_at_percent_rank_2_percent_rank").alias("max_event_at_percent_rank_2_percent_rank_check"),
    )
    .filter(F.col("max_event_at_percent_rank_2_percent_rank_check") > 0.3)
    .drop("max_event_at_percent_rank_2_percent_rank_check")
)

metrics_2 = (
    summary
    .select(
        "rn_dense_rank",
        "event_type_list",
        F.round(F.cume_dist().over(w_min_rn_rn_dense_rank_by_n_ev), 3).alias("n_event_type_replied_cume_dist"),
        F.ntile(10).over(w_min_rn_rn_dense_rank_by_n_ev_2).alias("n_event_type_replied_decile"),
    )
)

result = (
    metrics_2
    .select(
        "rn_dense_rank",
        "event_type_list",
        (F.floor(F.col("rn_dense_rank") / 2) * 2).alias("rn_dense_rank_bucket"),
        F.abs(F.col("rn_dense_rank") - 6).alias("rn_dense_rank_distance"),
    )
    .orderBy(
        F.col("rn_dense_rank_distance").asc_nulls_last(),
        F.col("rn_dense_rank").desc(),
    )
)
