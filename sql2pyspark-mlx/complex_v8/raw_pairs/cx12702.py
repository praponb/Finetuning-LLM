from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_year_running = Window.orderBy(F.col("event_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_year_ties = Window.orderBy(F.col("event_at_year").desc())
w_event_type_by_event_at_year = Window.partitionBy("event_type").orderBy(F.col("event_at_year").desc_nulls_first(), "id")
w_all_by_event_at_year = Window.orderBy(F.col("event_at_year").desc(), "id")
w_event_type_by_event_at_year_2 = Window.partitionBy("event_type").orderBy(F.col("event_at_year").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_event_at_year_3 = Window.partitionBy("event_type").orderBy(F.col("event_at_year").desc_nulls_first())
w_ticket_closed_at_year_ticket = Window.partitionBy(F.year("ticket_closed_at"), "ticket_id").orderBy("ticket_closed_at")
w_ticket_closed_at_year_ticket_2 = Window.partitionBy(F.year("ticket_closed_at"), "ticket_id").orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_year_full = Window.orderBy(F.col("event_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_at_year_dense_ra = Window.partitionBy("actor", "event_at_year_dense_rank")
w_second_ticket_closed_at_year = Window.partitionBy(F.year("second_ticket_closed_at"), "event_at_year_rank").orderBy(F.col("first_event_at_year").desc())
w_second_ticket_closed_at_year_2 = Window.partitionBy(F.year("second_ticket_closed_at"), "event_at_year_rank").orderBy(F.col("first_event_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_second_ticket_clos = Window.partitionBy("ticket_id", F.year("second_ticket_closed_at")).orderBy(F.col("first_event_at_year").desc(), "id").rowsBetween(-3, -1)
w_second_ticket_closed_at_year_3 = Window.partitionBy(F.year("second_ticket_closed_at"), "event_at_year_rank").orderBy(F.col("first_event_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_second_ticket_clos_2 = Window.partitionBy("ticket_id", F.year("second_ticket_closed_at")).orderBy(F.col("first_event_at_year").desc(), "id").rowsBetween(-6, Window.currentRow)
w_ticket_id_second_ticket_clos_3 = Window.partitionBy("ticket_id", F.year("second_ticket_closed_at")).orderBy(F.col("first_event_at_year").desc(), "id")
w_distinct_ids_by_median_ticke = Window.partitionBy("distinct_ids").orderBy(F.col("median_ticket_closed_at_cume_dist_avg_3_prior").desc(), "actor", "second_last_ticket_closed_at_ym")
w_actor = Window.partitionBy("actor")
w_distinct_ids_by_median_ticke_2 = Window.partitionBy("distinct_ids").orderBy(F.col("median_ticket_closed_at_cume_dist_avg_3_prior").desc())

valid_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("year", F.col("event_at")).alias("event_at_year"),
    )
)

enriched = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.month(F.col("t.closed_at")).isin(2, 3))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_year",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.last("event_at_year", ignorenulls=True).over(w_all_by_event_at_year_running).alias("last_known_event_at_year"),
        F.rank().over(w_all_by_event_at_year_ties).alias("event_at_year_rank"),
        F.lead("event_at").over(w_event_type_by_event_at_year).alias("next_event_at"),
        F.round((F.unix_timestamp("event_at_year") - F.unix_timestamp(F.lag("event_at_year").over(w_all_by_event_at_year))) / 60, 1).alias("minutes_since_prev_event_at_year"),
        F.last("ticket_closed_at").over(w_event_type_by_event_at_year_2).alias("last_ticket_closed_at"),
        F.round(F.cume_dist().over(w_event_type_by_event_at_year_3), 3).alias("event_at_year_cume_dist"),
        F.ntile(10).over(w_event_type_by_event_at_year).alias("event_at_year_decile"),
        F.dense_rank().over(w_event_type_by_event_at_year_3).alias("event_at_year_dense_rank"),
        F.row_number().over(w_event_type_by_event_at_year).alias("rn"),
        F.round(F.cume_dist().over(w_ticket_closed_at_year_ticket), 3).alias("ticket_closed_at_cume_dist"),
        F.row_number().over(w_all_by_event_at_year).alias("seq_no"),
        F.first("event_at_year").over(w_ticket_closed_at_year_ticket_2).alias("first_event_at_year"),
        F.lag("ticket_closed_at", 3).over(w_event_type_by_event_at_year).alias("prev3_ticket_closed_at"),
        F.nth_value("ticket_closed_at", 2).over(w_all_by_event_at_year_full).alias("second_ticket_closed_at"),
    )
)

with_trends = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_at_year",
        "ticket_tags",
        "last_known_event_at_year",
        "next_event_at",
        "last_ticket_closed_at",
        "event_at_year_cume_dist",
        "event_at_year_decile",
        "ticket_closed_at_cume_dist",
        "first_event_at_year",
        "second_ticket_closed_at",
        F.count(F.lit(1)).over(w_actor_event_at_year_dense_ra).alias("rows_in_actor_event_at_year_dense_rank"),
        F.dense_rank().over(w_second_ticket_closed_at_year).alias("first_event_at_year_dense_rank"),
        F.count(F.lit(1)).over(w_second_ticket_closed_at_year_2).alias("running_count"),
        F.round(F.avg("ticket_closed_at_cume_dist").over(w_ticket_id_second_ticket_clos), 2).alias("ticket_closed_at_cume_dist_avg_3_prior"),
        F.nth_value("last_ticket_closed_at", 2).over(w_second_ticket_closed_at_year_3).alias("second_last_ticket_closed_at"),
        F.sum("event_at_year_dense_rank").over(w_ticket_id_second_ticket_clos_2).alias("event_at_year_dense_rank_sum_7"),
        F.lead("last_known_event_at_year").over(w_ticket_id_second_ticket_clos_3).alias("next_last_known_event_at_year"),
    )
)

grouped = (
    with_trends
    .filter(F.col("rows_in_actor_event_at_year_dense_rank") < 16)
    .groupBy(
        "actor",
        F.date_format("second_last_ticket_closed_at", "yyyy-MM").alias("second_last_ticket_closed_at_ym"),
    )
    .agg(
        F.min("last_known_event_at_year").alias("first_last_known_event_at_year"),
        F.round(F.avg("event_at_year_decile"), 2).alias("avg_event_at_year_decile"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("ticket_closed_at_cume_dist_avg_3_prior", 0.5).alias("median_ticket_closed_at_cume_dist_avg_3_prior"),
    )
)

with_windows = (
    grouped
    .select(
        "*",
        (F.col("cnt") - F.lag("cnt").over(w_distinct_ids_by_median_ticke)).alias("cnt_change"),
        F.count(F.lit(1)).over(w_actor).alias("rows_in_actor"),
        F.round(F.cume_dist().over(w_distinct_ids_by_median_ticke_2), 3).alias("median_ticket_closed_at_cume_dist_avg_3_prior_cu"),
    )
)

result = (
    with_windows
    .filter(F.col("rows_in_actor") <= 14)
    .select(
        "actor",
        "second_last_ticket_closed_at_ym",
        "first_last_known_event_at_year",
        "avg_event_at_year_decile",
        "cnt",
        "distinct_ids",
        "median_ticket_closed_at_cume_dist_avg_3_prior",
        "median_ticket_closed_at_cume_dist_avg_3_prior_cu",
    )
    .orderBy(F.col("cnt").desc_nulls_first())
)
