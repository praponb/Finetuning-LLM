from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_ms_played = Window.partitionBy("is_skipped").orderBy(F.col("ms_played").desc(), "id")
w_track_id_source_by_played_at = Window.partitionBy("track_id", "source").orderBy(F.col("played_at").desc_nulls_first(), "id")
w_ms_played_decile_listener_id = Window.partitionBy("ms_played_decile", "listener_id").orderBy("prev3_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ms_played_decile_listener_id_2 = Window.partitionBy("ms_played_decile", "listener_id").orderBy("prev3_ms_played", "id")
w_listener_id = Window.partitionBy("listener_id")
w_is_skipped_played_at_month_b = Window.partitionBy("is_skipped", F.month("played_at")).orderBy(F.col("prev3_ms_played").asc_nulls_last(), "id")
w_ms_played_decile_listener_id_3 = Window.partitionBy("ms_played_decile", "listener_id").orderBy("prev3_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_plays
    .filter(F.col("source") != "search")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("track_id").cast("string")).alias("source_ref"),
    )
)

metrics = (
    scoped
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.ntile(10).over(w_is_skipped_by_ms_played).alias("ms_played_decile"),
        F.lag("ms_played", 3).over(w_track_id_source_by_played_at).alias("prev3_ms_played"),
        F.round(F.percent_rank().over(w_is_skipped_by_ms_played), 3).alias("ms_played_percent_rank"),
    )
)

listener_id_stats = (
    metrics
    .groupBy("listener_id")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

above_average = (
    metrics.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

ranked = (
    above_average
    .select(
        "*",
        F.first("played_at", ignorenulls=True).over(w_ms_played_decile_listener_id).alias("first_known_played_at"),
        (F.col("avg_ms_played") - F.lag("avg_ms_played").over(w_ms_played_decile_listener_id_2)).alias("avg_ms_played_change"),
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        (F.col("ms_played_percent_rank") - F.lag("ms_played_percent_rank").over(w_is_skipped_played_at_month_b)).alias("ms_played_percent_rank_change"),
        F.last("ms_played").over(w_ms_played_decile_listener_id_3).alias("last_ms_played"),
    )
)

combined_details = (
    ranked.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.*",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.username").alias("listener_username"),
    )
)

with_parent_listener = (
    combined_details.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.ms_played_decile",
        "s.prev3_ms_played",
        "s.ms_played_minus_avg",
        "s.first_known_played_at",
        "s.rows_in_listener_id",
        "s.ms_played_percent_rank_change",
        "s.last_ms_played",
        "s.listener_signup_date",
        "s.listener_username",
        F.col("l.birth_year").alias("listener_birth_year_2"),
    )
)

derived = (
    with_parent_listener
    .filter(F.col("last_ms_played") < 302279)
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "ms_played_decile",
        "prev3_ms_played",
        "first_known_played_at",
        "rows_in_listener_id",
        "ms_played_percent_rank_change",
        "last_ms_played",
        "listener_birth_year_2",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.abs(F.col("listener_birth_year_2") - 1975).alias("listener_birth_year_2_distance"),
    )
)

result = (
    derived
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "ms_played_decile",
        "prev3_ms_played",
        "first_known_played_at",
        "rows_in_listener_id",
        "last_ms_played",
        "listener_birth_year_2",
        "listener_birth_year_2_distance",
    )
    .orderBy(F.col("ms_played_decile").desc(), F.col("prev3_ms_played").desc(), "id")
    .limit(20)
)
