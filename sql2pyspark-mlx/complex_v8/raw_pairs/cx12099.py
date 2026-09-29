from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        F.when(F.col("bpm") >= 142, "A").when(F.col("bpm") >= 76, "B").otherwise("C").alias("bpm_tier"),
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
)

bpm_tier_stats = (
    prepared_tracks
    .groupBy("bpm_tier")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

above_average = (
    prepared_tracks.alias("s")
    .join(bpm_tier_stats.alias("g"), F.col("g.bpm_tier") == F.col("s.bpm_tier"), "inner")
    .filter(F.col("s.duration_sec") > F.col("g.avg_duration_sec"))
    .select(
        "s.*",
        "g.avg_duration_sec",
        F.round(F.col("s.duration_sec") - F.col("g.avg_duration_sec"), 2).alias("duration_sec_minus_avg"),
    )
)

combined_details_artist = (
    above_average.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm_tier",
        "s.release_date_month",
        "s.avg_duration_sec",
        "s.duration_sec_minus_avg",
        "a.artist_name",
    )
)

qualified = (
    combined_details_artist
    .crossJoin(combined_details_artist.agg(F.avg("duration_sec_minus_avg").alias("overall_avg_duration_sec_minus_avg")))
    .filter(F.col("duration_sec_minus_avg") > F.col("overall_avg_duration_sec_minus_avg"))
    .drop("overall_avg_duration_sec_minus_avg")
)

artist_id_stats = (
    qualified
    .groupBy("artist_id")
    .agg(F.avg("duration_sec").alias("avg_duration_sec_2"))
)

compared = (
    qualified.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.duration_sec") > F.col("g.avg_duration_sec_2"))
    .select(
        "s.*",
        "g.avg_duration_sec_2",
        F.round(F.col("s.duration_sec") - F.col("g.avg_duration_sec_2"), 2).alias("duration_sec_minus_avg_2"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("device").isNotNull(), 1)).alias("play_device_count"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    compared.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

result = (
    summarized_rows
    .filter(F.col("play_count") < 6)
    .select(
        "id",
        "artist_id",
        "bpm_tier",
        "release_date_month",
        "avg_duration_sec",
        "duration_sec_minus_avg",
        "avg_duration_sec_2",
        "duration_sec_minus_avg_2",
        "play_device_count",
        "play_count",
        F.to_date(F.col("release_date_month")).alias("release_date_month_date"),
        F.coalesce(F.col("duration_sec_minus_avg_2"), F.lit(0.0)).alias("duration_sec_minus_avg_2_filled"),
    )
)
