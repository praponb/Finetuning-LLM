from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at_hour").asc_nulls_last(), "id")
w_event_at_hour_event_type_by = Window.partitionBy("event_at_hour", "event_type").orderBy("event_at_hour", "id")
w_event_type_by_days_since_eve = Window.partitionBy("event_type").orderBy(F.col("days_since_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_days_since_eve_2 = Window.partitionBy("event_type").orderBy(F.col("days_since_event_at").desc(), "id")
w_actor_event_type = Window.partitionBy("actor", "event_type")
w_event_type = Window.partitionBy("event_type")
w_event_at_hour_event_type_by_2 = Window.partitionBy("event_at_hour", "event_type").orderBy("event_at_hour", "id").rowsBetween(-6, Window.currentRow)
w_event_at_hour_event_type_by_3 = Window.partitionBy("event_at_hour", "event_type").orderBy(F.col("days_since_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_known_event_at_hour_tic = Window.partitionBy("last_known_event_at_hour", "ticket_id")
w_least_second_event_at_hour_e = Window.partitionBy("least_second_event_at_hour_event_at_hour", "event_at_hour")
w_first_known_event_at_hour_la = Window.partitionBy("first_known_event_at_hour", "last_known_event_at_hour").orderBy(F.col("first_known_event_at_hour").desc(), "id")
w_rn_by_least_second_event_at = Window.partitionBy("rn").orderBy("least_second_event_at_hour_event_at_hour", "id").rowsBetween(-3, Window.currentRow)
w_first_known_event_at_hour_la_2 = Window.partitionBy("first_known_event_at_hour", "last_known_event_at_hour").orderBy(F.col("first_known_event_at_hour").desc())
w_last_known_event_at_hour_eve = Window.partitionBy("last_known_event_at_hour", "event_at_hour_quartile").orderBy(F.col("least_second_event_at_hour_event_at_hour").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_ticket_events
    .filter((F.col("event_type") == "replied") & (F.col("actor") == "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.hour(F.col("event_at")).alias("event_at_hour"),
    )
)

windowed = (
    prepared
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_at_hour",
        F.ntile(4).over(w_actor_ticket_id_by_event_at).alias("event_at_hour_quartile"),
        F.row_number().over(w_event_at_hour_event_type_by).alias("rn"),
        F.nth_value("event_at_hour", 2).over(w_event_type_by_days_since_eve).alias("second_event_at_hour"),
        F.ntile(5).over(w_event_type_by_days_since_eve_2).alias("days_since_event_at_quintile"),
        F.min("days_since_event_at").over(w_actor_event_type).alias("actor_event_type_min_days_since_event_at"),
        F.min("days_since_event_at").over(w_event_type).alias("event_type_min_days_since_event_at"),
        F.round(F.avg("event_at_hour").over(w_event_at_hour_event_type_by_2), 2).alias("event_at_hour_avg_7"),
        F.max("days_since_event_at").over(w_event_type).alias("event_type_max_days_since_event_at"),
        F.round(F.col("event_at_hour") - F.avg("event_at_hour").over(w_event_type), 2).alias("event_at_hour_vs_event_type_avg"),
        F.first("event_at_hour", ignorenulls=True).over(w_event_at_hour_event_type_by_3).alias("first_known_event_at_hour"),
        F.last("event_at_hour", ignorenulls=True).over(w_event_at_hour_event_type_by_3).alias("last_known_event_at_hour"),
    )
)

annotated = (
    windowed
    .select(
        "id",
        "ticket_id",
        "event_at_hour",
        "event_at_hour_quartile",
        "rn",
        "actor_event_type_min_days_since_event_at",
        "event_type_min_days_since_event_at",
        "event_at_hour_avg_7",
        "event_type_max_days_since_event_at",
        "event_at_hour_vs_event_type_avg",
        "first_known_event_at_hour",
        "last_known_event_at_hour",
        F.least(F.col("second_event_at_hour"), F.col("event_at_hour")).alias("least_second_event_at_hour_event_at_hour"),
        F.abs(F.col("event_at_hour") - 12).alias("event_at_hour_distance"),
    )
)

metrics = (
    annotated
    .select(
        "*",
        F.count(F.lit(1)).over(w_last_known_event_at_hour_tic).alias("rows_in_last_known_event_at_hour_ticket_id"),
        F.count(F.lit(1)).over(w_least_second_event_at_hour_e).alias("rows_in_least_second_event_at_hour_event_at_hour"),
        F.lead("event_at_hour", 1, 0).over(w_first_known_event_at_hour_la).alias("next_event_at_hour"),
        F.sum("event_at_hour").over(w_rn_by_least_second_event_at).alias("event_at_hour_sum_4"),
        F.round(F.col("event_at_hour") / F.nullif(F.sum("event_at_hour").over(w_least_second_event_at_hour_e), F.lit(0)), 4).alias("event_at_hour_share_of_least_second_event_at_hou"),
        F.dense_rank().over(w_first_known_event_at_hour_la_2).alias("first_known_event_at_hour_dense_rank"),
        F.min("event_at_hour").over(w_least_second_event_at_hour_e).alias("least_second_event_at_hour_event_at_hour_event_a"),
        F.nth_value("event_at_hour", 2).over(w_last_known_event_at_hour_eve).alias("second_event_at_hour"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ticket_id",
        "event_at_hour_quartile",
        "actor_event_type_min_days_since_event_at",
        "event_type_min_days_since_event_at",
        "event_at_hour_avg_7",
        "event_at_hour_vs_event_type_avg",
        "first_known_event_at_hour",
        "last_known_event_at_hour",
        "least_second_event_at_hour_event_at_hour",
        "event_at_hour_distance",
        "rows_in_last_known_event_at_hour_ticket_id",
        "rows_in_least_second_event_at_hour_event_at_hour",
        "next_event_at_hour",
        "event_at_hour_share_of_least_second_event_at_hou",
        "least_second_event_at_hour_event_at_hour_event_a",
        "second_event_at_hour",
        F.least(F.col("next_event_at_hour"), F.col("second_event_at_hour")).alias("least_next_event_at_hour_second_event_at_hour"),
        F.coalesce(F.col("second_event_at_hour"), F.lit(0)).alias("second_event_at_hour_filled"),
    )
    .orderBy(
        F.col("rows_in_last_known_event_at_hour_ticket_id").asc_nulls_last(),
        F.col("least_second_event_at_hour_event_at_hour").desc(),
        "id",
    )
    .limit(10)
)
