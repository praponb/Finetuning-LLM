from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moods_count_by_bpm_full = Window.partitionBy("moods_count").orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id = Window.partitionBy("artist_id")
w_moods_count_by_bpm_running = Window.partitionBy("moods_count").orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_artist_id_by_bpm = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm_minus_duration_sec").desc_nulls_first())
w_is_explicit_artist_id_by_bpm_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm_minus_duration_sec").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_is_explicit_artist_id_by_bpm_3 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm_minus_duration_sec").desc_nulls_first(), "id")
w_is_explicit = Window.partitionBy("is_explicit")
w_is_explicit_artist_id_by_bpm_4 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm_minus_duration_sec").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("S%")) | (F.col("duration_sec") < 352))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.size(F.col("moods")).alias("moods_count"),
        (F.col("bpm") - F.col("duration_sec")).alias("bpm_minus_duration_sec"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("ms_played").alias("min_play_ms_played"),
        F.count("*").alias("play_count"),
    )
)

with_counts = (
    prepared_tracks.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.min_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

sequenced = (
    with_counts
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "moods_count",
        "bpm_minus_duration_sec",
        "min_play_ms_played",
        F.nth_value("min_play_ms_played", 2).over(w_moods_count_by_bpm_full).alias("second_min_play_ms_played"),
        F.round(F.col("play_count") - F.avg("play_count").over(w_artist_id), 2).alias("play_count_vs_artist_id_avg"),
        F.max("bpm_minus_duration_sec").over(w_artist_id).alias("artist_id_max_bpm_minus_duration_sec"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.sum("bpm").over(w_moods_count_by_bpm_running).alias("running_bpm"),
    )
)

windowed = (
    sequenced
    .select(
        "*",
        F.dense_rank().over(w_is_explicit_artist_id_by_bpm).alias("bpm_minus_duration_sec_dense_rank"),
        F.max("bpm").over(w_is_explicit_artist_id_by_bpm_2).alias("bpm_max_2_prior"),
        F.ntile(4).over(w_is_explicit_artist_id_by_bpm_3).alias("bpm_minus_duration_sec_quartile"),
        F.size(F.collect_set("artist_id").over(w_is_explicit)).alias("distinct_artist_id_in_is_explicit"),
        F.nth_value("rows_in_artist_id", 2).over(w_is_explicit_artist_id_by_bpm_4).alias("second_rows_in_artist_id"),
    )
)

artist_id_stats = (
    windowed
    .groupBy("artist_id")
    .agg(F.percentile_approx("rows_in_artist_id", 0.5).alias("median_rows_in_artist_id"))
)

compared = (
    windowed.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.rows_in_artist_id") >= F.col("g.median_rows_in_artist_id"))
    .select(
        "s.*",
        "g.median_rows_in_artist_id",
        F.round(F.col("s.rows_in_artist_id") - F.col("g.median_rows_in_artist_id"), 2).alias("rows_in_artist_id_minus_median"),
    )
)

totals = (
    compared
    .groupBy(
        F.when(F.col("bpm_max_2_prior") >= 420, "large").when(F.col("bpm_max_2_prior") >= 252, "medium").otherwise("small").alias("bpm_max_2_prior_tier"),
        "artist_id",
        "bpm_minus_duration_sec_quartile",
    )
    .agg(
        F.sum("rows_in_artist_id_minus_median").alias("total_rows_in_artist_id_minus_median"),
        F.count("*").alias("cnt"),
        F.round(F.avg("second_rows_in_artist_id"), 2).alias("avg_second_rows_in_artist_id"),
    )
)

result = (
    totals
    .select(
        "bpm_max_2_prior_tier",
        "artist_id",
        "avg_second_rows_in_artist_id",
        "total_rows_in_artist_id_minus_median",
    )
    .unpivot(
        ids=["bpm_max_2_prior_tier", "artist_id"],
        values=["avg_second_rows_in_artist_id", "total_rows_in_artist_id_minus_median"],
        variableColumnName="field",
        valueColumnName="amount",
    )
)
