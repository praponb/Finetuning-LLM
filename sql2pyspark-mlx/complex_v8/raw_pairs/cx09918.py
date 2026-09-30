from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_by_event_at = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc_nulls_first(), "id")
w_event_at_month_by_event_at_f = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_at_month_by_event_at_t = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy(F.col("event_at").desc())
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_dense_rank_2_event = Window.partitionBy("event_at_dense_rank_2", "event_at_dense_rank_4")
w_event_at_dense_rank_4_last_e = Window.partitionBy("event_at_dense_rank_4", F.to_date("last_event_at_2"))

filtered_events = (
    df_ticket_events
    .filter(F.col("event_type") == "replied")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(" / ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

windowed = (
    filtered_events
    .select(
        "*",
        F.ntile(3).over(w_event_at_month_by_event_at).alias("event_at_tercile"),
        F.ntile(5).over(w_all_by_event_at).alias("event_at_quintile"),
        F.dense_rank().over(w_all_by_event_at).alias("event_at_dense_rank"),
        F.lead("event_at").over(w_event_at_month_by_event_at).alias("next_event_at"),
        F.last("event_at").over(w_event_at_month_by_event_at_f).alias("last_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.dense_rank().over(w_event_at_month_by_event_at).alias("event_at_dense_rank_2"),
        F.row_number().over(w_event_type_by_event_at).alias("event_at_seq"),
        F.ntile(4).over(w_all_by_event_at).alias("event_at_quartile"),
        F.dense_rank().over(w_event_at_month_by_event_at_t).alias("event_at_dense_rank_3"),
        F.dense_rank().over(w_event_type_by_event_at).alias("event_at_dense_rank_4"),
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
        F.lag("event_at").over(w_event_type_by_event_at).alias("prev_event_at"),
        F.last("event_at").over(w_event_type_by_event_at_full).alias("last_event_at_2"),
    )
)

annotated = (
    windowed
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_actor_label",
        "event_at_tercile",
        "event_at_quintile",
        "event_at_dense_rank",
        "next_event_at",
        "event_at_dense_rank_2",
        "event_at_quartile",
        "event_at_dense_rank_4",
        "event_at_cume_dist",
        "last_event_at_2",
        F.round(F.col("event_at_dense_rank_3") / F.nullif(F.col("event_at_quartile"), F.lit(0)), 3).alias("event_at_dense_rank_3_per_event_at_quartile"),
    )
)

windowed_2 = (
    annotated
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_actor_label",
        "event_at_tercile",
        "event_at_quintile",
        "event_at_dense_rank",
        "event_at_dense_rank_2",
        "event_at_quartile",
        "event_at_dense_rank_4",
        "last_event_at_2",
        F.round(F.col("event_at_quintile") - F.avg("event_at_quintile").over(w_event_at_dense_rank_2_event), 2).alias("event_at_quintile_vs_event_at_dense_rank_2_event"),
        F.size(F.collect_set("ticket_id").over(w_event_at_dense_rank_4_last_e)).alias("distinct_ticket_id_in_event_at_dense_rank_4_last"),
        F.min("event_at_quintile").over(w_event_at_dense_rank_4_last_e).alias("event_at_dense_rank_4_last_event_at_2_to_date_mi"),
        F.count(F.lit(1)).over(w_event_at_dense_rank_2_event).alias("rows_in_event_at_dense_rank_2_event_at_dense_ran"),
        F.count(F.lit(1)).over(w_event_at_dense_rank_4_last_e).alias("rows_in_event_at_dense_rank_4_last_event_at_2_to"),
        F.size(F.collect_set("actor").over(w_event_at_dense_rank_2_event)).alias("distinct_actor_in_event_at_dense_rank_2_event_at"),
    )
)

per_group = (
    windowed_2
    .groupBy("event_at_dense_rank_4")
    .agg(
        F.round(F.avg("event_at_quintile_vs_event_at_dense_rank_2_event"), 2).alias("avg_event_at_quintile_vs_event_at_dense_rank_2_e"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("event_at_quartile").alias("max_event_at_quartile_check"),
    )
    .filter(F.col("max_event_at_quartile_check") > 3)
    .drop("max_event_at_quartile_check")
)

result = (
    per_group
    .select(
        "event_at_dense_rank_4",
        "distinct_ids",
        F.least(F.col("distinct_ids"), F.col("event_at_dense_rank_4")).alias("least_distinct_ids_event_at_dense_rank_4"),
        F.abs(F.col("event_at_dense_rank_4") - 7).alias("event_at_dense_rank_4_distance"),
    )
    .orderBy(
        F.col("least_distinct_ids_event_at_dense_rank_4").desc_nulls_first(),
        "event_at_dense_rank_4",
    )
    .limit(5)
)
