from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_month_by_listener = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy("listener_birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_month_by_listener_2 = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy("listener_birth_year", "id").rowsBetween(-1, Window.currentRow)
w_listener_id_track_id = Window.partitionBy("listener_id", "track_id")
w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_listener_birth_year = Window.orderBy("listener_birth_year", "id")
w_listener_id = Window.partitionBy("listener_id")
w_all_by_played_at_prior_2 = Window.orderBy("played_at", "id").rowsBetween(-2, -1)
w_played_at_month_by_listener_3 = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy("listener_birth_year", "id")

valid_plays = (
    df_plays
    .filter((F.col("device") != "desktop") & (F.col("played_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
    )
)

with_parent = (
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.array_contains(F.col("l.interests"), "jazz"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

with_windows = (
    with_parent
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "listener_interests",
        "listener_birth_year",
        F.first("listener_birth_year", ignorenulls=True).over(w_played_at_month_by_listener).alias("first_known_listener_birth_year"),
        F.sum("listener_birth_year").over(w_played_at_month_by_listener_2).alias("listener_birth_year_sum_2"),
        F.round(F.col("listener_birth_year") / F.nullif(F.sum("listener_birth_year").over(w_listener_id_track_id), F.lit(0)), 4).alias("listener_birth_year_share_of_listener_id_track_i"),
        F.count(F.lit(1)).over(w_all_by_played_at_running).alias("running_count"),
        F.round(F.cume_dist().over(w_all_by_listener_birth_year), 3).alias("listener_birth_year_cume_dist"),
        F.size(F.collect_set("track_id").over(w_listener_id)).alias("distinct_track_id_in_listener_id"),
        F.last("listener_birth_year", ignorenulls=True).over(w_played_at_month_by_listener).alias("last_known_listener_birth_year"),
        F.round(F.avg("listener_birth_year").over(w_all_by_played_at_prior_2), 2).alias("listener_birth_year_avg_2_prior"),
        F.row_number().over(w_played_at_month_by_listener_3).alias("seq_no"),
    )
)

top_per_group = (
    with_windows
    .filter((F.col("seq_no") <= 5) & (F.col("played_at") < F.lit("2025-03-02 10:00:00").cast("timestamp")))
    .drop("seq_no")
)

result = (
    top_per_group
    .filter(F.col("listener_birth_year_sum_2").between(2366, 5414))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "listener_interests",
        "listener_birth_year",
        "first_known_listener_birth_year",
        "listener_birth_year_sum_2",
        "listener_birth_year_share_of_listener_id_track_i",
        "running_count",
        "distinct_track_id_in_listener_id",
        "listener_birth_year_avg_2_prior",
        F.greatest(F.col("listener_birth_year"), F.col("running_count")).alias("greatest_listener_birth_year_running_count"),
        F.coalesce(F.col("listener_birth_year"), F.lit(0)).alias("listener_birth_year_filled"),
    )
    .orderBy("track_id")
)
