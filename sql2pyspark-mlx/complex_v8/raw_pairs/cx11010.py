from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_tercile_event_at_to = Window.partitionBy("event_at_tercile", F.to_date("event_at"))
w_all_by_event_at_tercile_runn = Window.orderBy("event_at_tercile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("event_type").orderBy("event_at_tercile", "id")
w_event_at_tercile_by_event_at = Window.partitionBy("event_at_tercile").orderBy("event_at", "id")
w_second_event_at_year_event_a = Window.partitionBy(F.year("second_event_at"), "event_at_tercile").orderBy(F.col("event_at_tercile").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_event_at_month_by_eve = Window.partitionBy(F.month("second_event_at")).orderBy("event_at_percent_rank")

cleaned = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        F.ntile(3).over(w_all_by_event_at).alias("event_at_tercile"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.lead("event_at").over(w_all_by_event_at).alias("next_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
    )
)

with_trends = (
    with_windows
    .select(
        "id",
        "event_at",
        "event_type",
        "event_at_tercile",
        "event_at_percent_rank",
        "event_at_cume_dist",
        "second_event_at",
        F.round(F.col("event_at_tercile") / F.nullif(F.sum("event_at_tercile").over(w_event_at_tercile_event_at_to), F.lit(0)), 4).alias("event_at_tercile_share_of_event_at_tercile_event"),
        F.round(F.col("event_at_cume_dist") / F.nullif(F.sum("event_at_cume_dist").over(w_event_at_tercile_event_at_to), F.lit(0)), 4).alias("event_at_cume_dist_share_of_event_at_tercile_eve"),
        F.first("event_at_percent_rank", ignorenulls=True).over(w_all_by_event_at_tercile_runn).alias("first_known_event_at_percent_rank"),
    )
)

latest = (
    with_trends
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_trends_2 = (
    latest
    .select(
        "*",
        F.lag("event_at_tercile", 1, 0).over(w_event_at_tercile_by_event_at).alias("prev_event_at_tercile"),
        F.last("second_event_at").over(w_second_event_at_year_event_a).alias("last_second_event_at"),
        F.dense_rank().over(w_second_event_at_month_by_eve).alias("event_at_percent_rank_dense_rank"),
        F.lead("event_at_percent_rank").over(w_event_at_tercile_by_event_at).alias("next_event_at_percent_rank"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "event_at_tercile",
        "event_at_percent_rank",
        "event_at_cume_dist",
        "second_event_at",
        "event_at_cume_dist_share_of_event_at_tercile_eve",
        "first_known_event_at_percent_rank",
        "prev_event_at_tercile",
        "last_second_event_at",
        "event_at_percent_rank_dense_rank",
        "next_event_at_percent_rank",
        (F.col("event_at_cume_dist") - F.col("prev_event_at_tercile")).alias("event_at_cume_dist_minus_prev_event_at_tercile"),
        F.date_format(F.col("last_second_event_at"), "yyyy-MM-dd HH:00").alias("last_second_event_at_label"),
    )
    .orderBy(F.col("event_at_cume_dist").desc_nulls_first(), "id")
    .limit(5)
)
