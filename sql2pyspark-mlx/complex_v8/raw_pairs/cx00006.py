from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_type_event_at_year_by = Window.partitionBy("event_type", F.year("event_at")).orderBy(F.col("event_at").desc(), "id")
w_event_type_event_at_year_by_2 = Window.partitionBy("event_type", F.year("event_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").asc_nulls_last())
w_event_at_month_event_type_by = Window.partitionBy(F.month("event_at"), "event_type").orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_event_type = Window.partitionBy("ticket_id", "event_type")
w_event_type_by_event_at_cume = Window.partitionBy("event_type").orderBy("event_at_cume_dist", "id")
w_row_count_event_type = Window.partitionBy("row_count", "event_type")
w_row_count_by_median_event_at = Window.partitionBy("row_count").orderBy("median_event_at_cume_dist", "next2_event_at_year", "actor", "event_type")

cleaned = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_event_at_year_by))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.nth_value("event_at", 2).over(w_event_type_event_at_year_by_2).alias("second_event_at"),
        F.round(F.percent_rank().over(w_actor_by_event_at), 3).alias("event_at_percent_rank"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.first("event_at", ignorenulls=True).over(w_event_at_month_event_type_by).alias("first_known_event_at"),
        F.lead("event_at", 2).over(w_event_type_event_at_year_by).alias("next2_event_at"),
    )
)

with_windows = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_at_label",
        "minutes_since_prev_event_at_2",
        "second_event_at",
        "event_at_cume_dist",
        "next2_event_at",
        F.round(F.col("event_at_cume_dist") / F.nullif(F.sum("event_at_cume_dist").over(w_ticket_id_event_type), F.lit(0)), 4).alias("event_at_cume_dist_share_of_ticket_id_event_type"),
        F.round(F.percent_rank().over(w_event_type_by_event_at_cume), 3).alias("event_at_cume_dist_percent_rank"),
        F.round((F.unix_timestamp("first_known_event_at") - F.unix_timestamp(F.lag("first_known_event_at").over(w_event_type_by_event_at_cume))) / 60, 1).alias("minutes_since_prev_first_known_event_at"),
    )
)

totals = (
    with_windows
    .filter(F.col("event_at_cume_dist") < 0.8)
    .groupBy(
        F.year("next2_event_at").alias("next2_event_at_year"),
        "actor",
        "event_type",
    )
    .agg(
        F.round(F.sum("event_at_cume_dist") / F.nullif(F.sum("minutes_since_prev_event_at_2"), F.lit(0)), 3).alias("event_at_cume_dist_to_minutes_since_prev_event_a"),
        F.percentile_approx("event_at_cume_dist", 0.5).alias("median_event_at_cume_dist"),
        F.max("next2_event_at").alias("last_next2_event_at"),
        F.round(F.avg("minutes_since_prev_first_known_event_at"), 2).alias("avg_minutes_since_prev_first_known_event_at"),
        F.count("*").alias("row_count"),
    )
)

with_trends_2 = (
    totals
    .select(
        "next2_event_at_year",
        "actor",
        "event_type",
        "event_at_cume_dist_to_minutes_since_prev_event_a",
        "median_event_at_cume_dist",
        "last_next2_event_at",
        "avg_minutes_since_prev_first_known_event_at",
        F.sum("median_event_at_cume_dist").over(w_row_count_event_type).alias("row_count_event_type_sum_median_event_at_cume_di"),
        F.round((F.unix_timestamp("last_next2_event_at") - F.unix_timestamp(F.lag("last_next2_event_at").over(w_row_count_by_median_event_at))) / 60, 1).alias("minutes_since_prev_last_next2_event_at"),
        F.lead("last_next2_event_at").over(w_row_count_by_median_event_at).alias("next_last_next2_event_at"),
    )
)

result = (
    with_trends_2
    .filter(F.col("event_at_cume_dist_to_minutes_since_prev_event_a") <= 8.0)
    .select(
        "next2_event_at_year",
        "actor",
        "event_type",
        "median_event_at_cume_dist",
        "last_next2_event_at",
        "avg_minutes_since_prev_first_known_event_at",
        "row_count_event_type_sum_median_event_at_cume_di",
        "minutes_since_prev_last_next2_event_at",
        "next_last_next2_event_at",
    )
    .orderBy(
        F.col("actor").desc_nulls_first(),
        "avg_minutes_since_prev_first_known_event_at",
    )
)
