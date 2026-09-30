from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy("event_at")
w_event_at_year_event_type_by = Window.partitionBy(F.year("event_at"), "event_type").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_or_unknown_ticket_id_b = Window.partitionBy("actor_or_unknown", "ticket_id").orderBy("event_at", "id")
w_all_by_event_at_cume_dist = Window.orderBy(F.col("event_at_cume_dist").desc(), "id")
w_second_event_at_year_event_a = Window.partitionBy(F.year("second_event_at"), "event_at_rank_2").orderBy(F.col("next_event_at").asc_nulls_last(), "id")
w_next_event_at_month = Window.partitionBy(F.date_trunc("month", "next_event_at"))
w_event_at_rank_event_at_quart = Window.partitionBy("event_at_rank", "event_at_quartile").orderBy(F.col("second_event_at").desc_nulls_first(), "id")
w_min_event_at_rank = Window.partitionBy("min_event_at_rank")
w_prev_event_at_month_month_by = Window.partitionBy(F.month("prev_event_at_month")).orderBy("max_next_event_at_month_sum_event_at_cume_dist", "next_event_at_2_ym", "prev_event_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_prev_event_at_month_month_by_2 = Window.partitionBy(F.month("prev_event_at_month")).orderBy("max_next_event_at_month_sum_event_at_cume_dist")

prepared_events = (
    df_ticket_events
    .filter((F.col("actor").isNotNull()) & (F.month(F.col("event_at")).isin(1, 2)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

metrics = (
    prepared_events
    .select(
        "*",
        F.lead("event_at").over(w_ticket_id_by_event_at).alias("next_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.ntile(5).over(w_ticket_id_by_event_at).alias("event_at_quintile"),
        F.lag("event_at").over(w_ticket_id_by_event_at).alias("prev_event_at"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.rank().over(w_ticket_id_by_event_at).alias("event_at_rank"),
        F.ntile(4).over(w_event_at_year_event_type_by).alias("event_at_quartile"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at"),
        F.round(F.percent_rank().over(w_actor_or_unknown_ticket_id_b), 3).alias("event_at_percent_rank"),
        F.first("event_at").over(w_ticket_id_by_event_at_full).alias("first_event_at"),
        F.rank().over(w_ticket_id_by_event_at_ties).alias("event_at_rank_2"),
        F.lead("event_at").over(w_actor_or_unknown_ticket_id_b).alias("next_event_at_2"),
    )
)

top_per_group = metrics.filter(F.col("event_at_rank_2") <= 5)

with_trends = (
    top_per_group
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "next_event_at",
        "event_at_quintile",
        "prev_event_at",
        "event_at_cume_dist",
        "event_at_rank",
        "event_at_quartile",
        "second_event_at",
        "event_at_percent_rank",
        "first_event_at",
        "event_at_rank_2",
        "next_event_at_2",
        F.lag("next_event_at", 2).over(w_all_by_event_at_cume_dist).alias("prev2_next_event_at"),
        F.round(F.percent_rank().over(w_all_by_event_at_cume_dist), 3).alias("event_at_cume_dist_percent_rank"),
        F.row_number().over(w_second_event_at_year_event_a).alias("rn"),
        F.size(F.collect_set("actor").over(w_next_event_at_month)).alias("distinct_actor_in_next_event_at_month"),
        F.row_number().over(w_event_at_rank_event_at_quart).alias("second_event_at_seq"),
        F.sum("event_at_cume_dist").over(w_next_event_at_month).alias("next_event_at_month_sum_event_at_cume_dist"),
    )
)

rollup_base = (
    with_trends
    .groupBy(
        F.date_format("next_event_at_2", "yyyy-MM").alias("next_event_at_2_ym"),
        F.date_trunc("month", "prev_event_at").alias("prev_event_at_month"),
    )
    .agg(
        F.max("next_event_at_month_sum_event_at_cume_dist").alias("max_next_event_at_month_sum_event_at_cume_dist"),
        F.min("event_at_rank").alias("min_event_at_rank"),
        F.max("event_at_quartile").alias("max_event_at_quartile_check"),
    )
    .filter(F.col("max_event_at_quartile_check") > 2)
    .drop("max_event_at_quartile_check")
)

ranked = (
    rollup_base
    .select(
        "next_event_at_2_ym",
        "prev_event_at_month",
        "max_next_event_at_month_sum_event_at_cume_dist",
        F.round(F.col("min_event_at_rank") / F.nullif(F.sum("min_event_at_rank").over(w_min_event_at_rank), F.lit(0)), 4).alias("min_event_at_rank_share_of_min_event_at_rank"),
        F.last("prev_event_at_month", ignorenulls=True).over(w_prev_event_at_month_month_by).alias("last_known_prev_event_at_month"),
        F.rank().over(w_prev_event_at_month_month_by_2).alias("max_next_event_at_month_sum_event_at_cume_dist_r"),
        F.max("min_event_at_rank").over(w_prev_event_at_month_month_by).alias("max_min_event_at_rank_so_far"),
    )
)

result = (
    ranked
    .filter(F.col("last_known_prev_event_at_month") >= F.lit("2025-01-13 09:00:00").cast("timestamp"))
    .select(
        "next_event_at_2_ym",
        "min_event_at_rank_share_of_min_event_at_rank",
        "last_known_prev_event_at_month",
        "max_next_event_at_month_sum_event_at_cume_dist_r",
        "max_min_event_at_rank_so_far",
        F.substring(F.trim(F.col("next_event_at_2_ym")), 1, 4).alias("next_event_at_2_ym_prefix"),
        F.length(F.trim(F.col("next_event_at_2_ym"))).alias("next_event_at_2_ym_len"),
    )
    .orderBy("last_known_prev_event_at_month")
)
