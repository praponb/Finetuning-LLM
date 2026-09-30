from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_len = Window.partitionBy("event_type_len")
w_event_type_event_at_to_date = Window.partitionBy("event_type", F.to_date("event_at")).orderBy(F.col("event_at").desc(), "id")
w_actor_by_event_type_len_runn = Window.partitionBy("actor").orderBy(F.col("event_type_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_event_at_year_by_event = Window.partitionBy("actor", F.year("event_at")).orderBy(F.col("event_type_len").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_actor_event_at_year_by_event_2 = Window.partitionBy("actor", F.year("event_at")).orderBy(F.col("event_type_len").desc_nulls_first())
w_actor_event_at_year_by_event_3 = Window.partitionBy("actor", F.year("event_at")).orderBy(F.col("event_type_len").desc_nulls_first(), "id")
w_event_type_len_ticket_id_by = Window.partitionBy("event_type_len", "ticket_id").orderBy(F.col("event_type_len").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_actor_by_event_type_len = Window.partitionBy("actor").orderBy(F.col("event_type_len").desc(), "id")
w_actor_event_at_year_by_event_4 = Window.partitionBy("actor", F.year("event_at")).orderBy(F.col("event_type_len").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_type_len_change = Window.orderBy("event_type_len_change_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_type_len_dense = Window.orderBy(F.col("event_type_len_dense_rank").desc_nulls_first())
w_event_type_len_dense_rank_ev = Window.partitionBy("event_type_len_dense_rank", "event_type_or_unknown")
w_all_by_event_type_len_change_2 = Window.orderBy("event_type_len_change_2", "id")
w_all_by_event_type_len_dense_2 = Window.orderBy(F.col("event_type_len_dense_rank").desc_nulls_first(), "id")

base = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(7, 9)) & (~F.col("actor").isin("agent", "customer")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

ranked = (
    base
    .select(
        "*",
        F.round(F.col("event_type_len") - F.avg("event_type_len").over(w_event_type_len), 2).alias("event_type_len_vs_event_type_len_avg"),
        F.lead("event_type_len", 2).over(w_event_type_event_at_to_date).alias("next2_event_type_len"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_event_at_to_date))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.count(F.lit(1)).over(w_actor_by_event_type_len_runn).alias("running_count"),
        F.sum("event_type_len").over(w_actor_event_at_year_by_event).alias("event_type_len_sum_6_prior"),
        F.dense_rank().over(w_actor_event_at_year_by_event_2).alias("event_type_len_dense_rank"),
        F.lead("event_at", 3).over(w_event_type_event_at_to_date).alias("next3_event_at"),
        F.round(F.percent_rank().over(w_actor_event_at_year_by_event_3), 3).alias("event_type_len_percent_rank"),
        F.round(F.avg("event_type_len").over(w_event_type_len_ticket_id_by), 2).alias("event_type_len_avg_4"),
        (F.col("event_type_len") - F.lag("event_type_len").over(w_actor_event_at_year_by_event_3)).alias("event_type_len_change"),
        (F.col("event_type_len") - F.lag("event_type_len").over(w_actor_by_event_type_len)).alias("event_type_len_change_2"),
        F.round(F.avg("event_type_len").over(w_event_type_len), 2).alias("event_type_len_avg_event_type_len"),
        F.last("event_at", ignorenulls=True).over(w_actor_by_event_type_len_runn).alias("last_known_event_at"),
        F.count(F.lit(1)).over(w_actor_event_at_year_by_event_4).alias("running_count_2"),
    )
)

scored = (
    ranked
    .filter(F.col("event_type_len_dense_rank") > 4)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_len",
        "event_type_or_unknown",
        "event_type_len_dense_rank",
        "next3_event_at",
        "event_type_len_percent_rank",
        "event_type_len_avg_4",
        "event_type_len_change_2",
        "event_type_len_avg_event_type_len",
        "last_known_event_at",
        "running_count_2",
        F.round(F.col("event_type_len_avg_4") * 1.07, 2).alias("event_type_len_avg_4_adj"),
        F.coalesce(F.col("event_type_len_change"), F.lit(0)).alias("event_type_len_change_filled"),
        F.round((F.unix_timestamp(F.col("last_known_event_at")) - F.unix_timestamp(F.col("next3_event_at"))) / 60, 1).alias("minutes_next3_event_at_to_last_known_event_at"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.nth_value("event_type_len_avg_4", 2).over(w_all_by_event_type_len_change).alias("second_event_type_len_avg_4"),
        F.dense_rank().over(w_all_by_event_type_len_dense).alias("event_type_len_dense_rank_dense_rank"),
        F.sum("event_type_len_percent_rank").over(w_event_type_len_dense_rank_ev).alias("event_type_len_dense_rank_event_type_or_unknown_"),
        F.lead("last_known_event_at").over(w_all_by_event_type_len_change_2).alias("next_last_known_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_type_len_change_2), 3).alias("event_type_len_change_2_cume_dist"),
        F.round(F.col("event_type_len_percent_rank") / F.nullif(F.sum("event_type_len_percent_rank").over(w_event_type_len_dense_rank_ev), F.lit(0)), 4).alias("event_type_len_percent_rank_share_of_event_type_"),
        F.lag("event_type_len_change_2", 1, 0).over(w_all_by_event_type_len_dense_2).alias("prev_event_type_len_change_2"),
    )
)

result = (
    with_windows
    .filter(F.col("prev_event_type_len_change_2") < 10)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_type_or_unknown",
        "next3_event_at",
        "event_type_len_percent_rank",
        "event_type_len_change_2",
        "event_type_len_avg_event_type_len",
        "running_count_2",
        "event_type_len_avg_4_adj",
        "event_type_len_change_filled",
        "minutes_next3_event_at_to_last_known_event_at",
        "second_event_type_len_avg_4",
        "event_type_len_dense_rank_dense_rank",
        "next_last_known_event_at",
        "event_type_len_change_2_cume_dist",
        "event_type_len_percent_rank_share_of_event_type_",
    )
    .orderBy("running_count_2", "id")
    .limit(10)
)
