from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_year = Window.partitionBy("release_date_year")
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy("duration_sec", "id")

scoped = (
    df_tracks
    .filter(F.year(F.col("release_date")) == 2025)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.year(F.col("release_date")).alias("release_date_year"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
    )
)

metrics = (
    scoped
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "release_date_year",
        "release_date_label",
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_release_date_year), 2).alias("duration_sec_vs_release_date_year_avg"),
        F.row_number().over(w_artist_id_by_duration_sec).alias("rn"),
    )
)

enriched_artist = (
    metrics.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.lower(F.col("a.artist_name")).like("%ig%")), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.release_date_year",
        "s.duration_sec_vs_release_date_year_avg",
        "s.rn",
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
    )
)

artist_genre_stats = (
    enriched_artist
    .groupBy("artist_genre")
    .agg(F.max("rn").alias("max_rn"))
)

compared = (
    enriched_artist.alias("s")
    .join(artist_genre_stats.alias("g"), F.col("g.artist_genre") == F.col("s.artist_genre"), "inner")
    .filter(F.col("s.rn") == F.col("g.max_rn"))
    .select(
        "s.*",
        "g.max_rn",
        F.round(F.col("s.rn") - F.col("g.max_rn"), 2).alias("rn_minus_max"),
    )
)

detail_rows = (
    compared.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date_year",
        "s.duration_sec_vs_release_date_year_avg",
        "s.rn",
        "s.artist_name",
        "s.max_rn",
        "s.rn_minus_max",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
    )
)

result = (
    detail_rows
    .groupingSets(
        [["release_date_year", "artist_id", "play_device"], ["release_date_year"], []],
        "release_date_year", "artist_id", "play_device",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("play_source").alias("distinct_play_sources"),
        F.array_sort(F.collect_set("play_device")).alias("play_device_list"),
    )
    .select(
        "release_date_year",
        "artist_id",
        F.coalesce(F.col("play_device"), F.lit("ALL")).alias("play_device"),
        "gid",
        "distinct_play_sources",
        "play_device_list",
    )
    .orderBy(
        F.col("release_date_year").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
        F.col("play_device").asc_nulls_last(),
    )
)
