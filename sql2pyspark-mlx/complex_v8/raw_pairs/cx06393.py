from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_quarter_is_expl = Window.partitionBy("release_date_quarter", "is_explicit").orderBy(F.col("release_date").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_release_date_quarter_is_expl_2 = Window.partitionBy("release_date_quarter", "is_explicit").orderBy(F.col("release_date").desc_nulls_first(), "id")
w_release_date_quarter_is_expl_3 = Window.partitionBy("release_date_quarter", "is_explicit").orderBy(F.col("release_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_quarter_is_expl_4 = Window.partitionBy("release_date_quarter", "is_explicit").orderBy(F.col("release_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_quarter_artist = Window.partitionBy("release_date_quarter", "artist_id")
w_release_date_dense_rank_is_e = Window.partitionBy("release_date_dense_rank", "is_explicit")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy("release_date_month", "id")

prepared_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.min("played_at").alias("first_played_at"),
    )
)

summarized_rows = (
    prepared_tracks.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.first_played_at",
    )
)

artist_id_stats = (
    summarized_rows
    .groupBy("artist_id")
    .agg(F.avg("play_count").alias("avg_play_count"))
)

compared = (
    summarized_rows.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.play_count") > F.col("g.avg_play_count"))
    .select(
        "s.*",
        "g.avg_play_count",
        F.round(F.col("s.play_count") - F.col("g.avg_play_count"), 2).alias("play_count_minus_avg"),
    )
)

screened = (
    compared.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.lower(F.col("a.artist_name")).like("% l%")), "left_semi")
)

with_trends = (
    screened
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "moods",
        "release_date_month",
        "play_count",
        "first_played_at",
        "avg_play_count",
        F.round(F.avg("play_count_minus_avg").over(w_release_date_quarter_is_expl), 2).alias("play_count_minus_avg_avg_4"),
        F.dense_rank().over(w_release_date_quarter_is_expl_2).alias("release_date_dense_rank"),
        F.first("release_date_month", ignorenulls=True).over(w_release_date_quarter_is_expl_3).alias("first_known_release_date_month"),
        F.last("first_played_at").over(w_release_date_quarter_is_expl_4).alias("last_first_played_at"),
        F.round(F.avg("release_date_quarter").over(w_release_date_quarter_artist), 2).alias("release_date_quarter_artist_id_avg_release_date_"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "release_date",
        "is_explicit",
        "release_date_month",
        "play_count",
        "first_played_at",
        "avg_play_count",
        "play_count_minus_avg_avg_4",
        "release_date_dense_rank",
        "first_known_release_date_month",
        "release_date_quarter_artist_id_avg_release_date_",
        F.round(F.avg("play_count_minus_avg_avg_4").over(w_release_date_dense_rank_is_e), 2).alias("release_date_dense_rank_is_explicit_avg_play_cou"),
        F.row_number().over(w_is_explicit_by_release_date).alias("rn"),
        F.max("play_count").over(w_release_date_dense_rank_is_e).alias("release_date_dense_rank_is_explicit_max_play_cou"),
    )
)

result = (
    metrics
    .select(
        "id",
        "release_date",
        "is_explicit",
        "release_date_month",
        "play_count",
        "first_played_at",
        "avg_play_count",
        "play_count_minus_avg_avg_4",
        "release_date_dense_rank",
        "first_known_release_date_month",
        "release_date_quarter_artist_id_avg_release_date_",
        "rn",
        "release_date_dense_rank_is_explicit_max_play_cou",
        (F.floor(F.col("release_date_dense_rank") / 2) * 2).alias("release_date_dense_rank_bucket"),
    )
    .orderBy(F.col("first_played_at").desc(), "release_date_dense_rank_bucket", "id")
    .limit(10)
)
