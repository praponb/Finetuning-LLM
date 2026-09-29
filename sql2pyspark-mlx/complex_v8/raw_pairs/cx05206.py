from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_played_at_month_b = Window.partitionBy("is_skipped", F.date_trunc("month", "played_at")).orderBy(F.col("listener_birth_year").asc_nulls_last(), "id")
w_listener_plan_listener_id_by = Window.partitionBy("listener_plan", "listener_id").orderBy("listener_signup_date")
w_listener_signup_date_month = Window.partitionBy(F.date_trunc("month", "listener_signup_date"))
w_track_id_listener_id = Window.partitionBy("track_id", "listener_id")
w_listener_plan_listener_id_by_2 = Window.partitionBy("listener_plan", "listener_id").orderBy("listener_signup_date", "id")
w_listener_plan_listener_id_by_3 = Window.partitionBy("listener_plan", "listener_id").orderBy("listener_birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_listener_plan = Window.partitionBy("is_skipped", "listener_plan")

base = (
    df_plays
    .filter((F.col("source").isNotNull()) & (F.col("played_at") >= F.lit("2025-02-06 15:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.concat(F.col("source"), F.lit("#"), F.col("track_id").cast("string")).alias("source_ref"),
    )
)

joined_listener = (
    base.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source_ref",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.plan").alias("listener_plan"),
    )
)

with_trends = (
    joined_listener
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source_ref",
        "listener_signup_date",
        "listener_plan",
        (F.col("listener_birth_year") - F.lag("listener_birth_year").over(w_is_skipped_played_at_month_b)).alias("listener_birth_year_change"),
        F.dense_rank().over(w_listener_plan_listener_id_by).alias("listener_signup_date_dense_rank"),
        F.sum("ms_played").over(w_listener_signup_date_month).alias("listener_signup_date_month_sum_ms_played"),
        F.count(F.lit(1)).over(w_track_id_listener_id).alias("rows_in_track_id_listener_id"),
        F.ntile(3).over(w_listener_plan_listener_id_by_2).alias("listener_signup_date_tercile"),
        F.nth_value("ms_played", 2).over(w_listener_plan_listener_id_by_3).alias("second_ms_played"),
        F.size(F.collect_set("track_id").over(w_is_skipped_listener_plan)).alias("distinct_track_id_in_is_skipped_listener_plan"),
        F.round(F.col("listener_birth_year") / F.nullif(F.sum("listener_birth_year").over(w_track_id_listener_id), F.lit(0)), 4).alias("listener_birth_year_share_of_track_id_listener_i"),
        F.size(F.collect_set("listener_id").over(w_is_skipped_listener_plan)).alias("distinct_listener_id_in_is_skipped_listener_plan"),
        F.count(F.lit(1)).over(w_listener_signup_date_month).alias("rows_in_listener_signup_date_month"),
    )
)

top_per_group = (
    with_trends
    .filter(F.col("listener_signup_date_dense_rank") <= 2)
    .drop("listener_signup_date_dense_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "listener_birth_year_change",
        "listener_signup_date_month_sum_ms_played",
        "second_ms_played",
        "distinct_track_id_in_is_skipped_listener_plan",
        "listener_birth_year_share_of_track_id_listener_i",
        "distinct_listener_id_in_is_skipped_listener_plan",
        "rows_in_listener_signup_date_month",
        F.coalesce(F.col("second_ms_played"), F.lit(0)).alias("second_ms_played_filled"),
    )
)
