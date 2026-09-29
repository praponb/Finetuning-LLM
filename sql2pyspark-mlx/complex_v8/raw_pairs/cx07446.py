from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_year_year_source = Window.partitionBy(F.year("played_at_year"), "source")
w_track_id_by_played_at_year_r = Window.partitionBy("track_id").orderBy(F.col("played_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_by_played_at_year_t = Window.partitionBy("track_id").orderBy(F.col("played_at_year").desc())
w_track_id_by_played_at_year = Window.partitionBy("track_id").orderBy(F.col("played_at_year").desc(), "id")

recent_plays = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.date_trunc("year", F.col("played_at")).alias("played_at_year"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.abs(F.col("ms_played") - 112786).alias("ms_played_distance"),
    )
)

windowed = (
    recent_plays
    .select(
        "id",
        "track_id",
        "played_at",
        "is_skipped",
        "played_at_year",
        "ms_played_bucket",
        F.size(F.collect_set("listener_id").over(w_played_at_year_year_source)).alias("distinct_listener_id_in_played_at_year_year_sour"),
        F.count(F.lit(1)).over(w_played_at_year_year_source).alias("rows_in_played_at_year_year_source"),
        F.first("played_at_year", ignorenulls=True).over(w_track_id_by_played_at_year_r).alias("first_known_played_at_year"),
        F.round(F.percent_rank().over(w_track_id_by_played_at_year_t), 3).alias("played_at_year_percent_rank"),
        F.dense_rank().over(w_track_id_by_played_at_year).alias("played_at_year_dense_rank"),
    )
)

exploded_detail = (
    windowed.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.track_id",
        "s.ms_played_bucket",
        "s.distinct_listener_id_in_played_at_year_year_sour",
        "s.first_known_played_at_year",
        "s.played_at_year_percent_rank",
        "s.played_at_year_dense_rank",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

summary = (
    exploded_detail
    .groupBy(
        F.date_format("first_known_played_at_year", "yyyy-MM").alias("first_known_played_at_year_ym"),
        "played_at_year_dense_rank",
        "play_listener_id",
    )
    .agg(
        F.round(F.sum("ms_played_bucket") / F.nullif(F.sum("played_at_year_dense_rank"), F.lit(0)), 3).alias("ms_played_bucket_to_played_at_year_dense_rank_ra"),
        F.round(F.stddev("played_at_year_dense_rank"), 2).alias("stddev_played_at_year_dense_rank"),
    )
)

result = (
    summary
    .select(
        "first_known_played_at_year_ym",
        "played_at_year_dense_rank",
        "play_listener_id",
        "ms_played_bucket_to_played_at_year_dense_rank_ra",
        "stddev_played_at_year_dense_rank",
    )
    .orderBy(
        F.col("play_listener_id").desc(),
        "first_known_played_at_year_ym",
        "played_at_year_dense_rank",
    )
    .limit(20)
)
