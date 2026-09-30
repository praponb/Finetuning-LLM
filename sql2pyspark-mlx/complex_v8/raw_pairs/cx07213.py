from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")

base_tracks = df_tracks.filter(F.col("bpm") < 155)

sequenced = (
    base_tracks
    .select(
        "*",
        F.round(F.col("bpm") - F.avg("bpm").over(w_is_explicit), 2).alias("bpm_vs_is_explicit_avg"),
        F.max("duration_sec").over(w_is_explicit).alias("is_explicit_max_duration_sec"),
    )
)

artist_id_stats = (
    sequenced
    .groupBy("artist_id")
    .agg(F.avg("bpm_vs_is_explicit_avg").alias("avg_bpm_vs_is_explicit_avg"))
)

outliers = (
    sequenced.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm_vs_is_explicit_avg") > F.col("g.avg_bpm_vs_is_explicit_avg"))
    .select(
        "s.*",
        "g.avg_bpm_vs_is_explicit_avg",
        F.round(F.col("s.bpm_vs_is_explicit_avg") - F.col("g.avg_bpm_vs_is_explicit_avg"), 2).alias("bpm_vs_is_explicit_avg_minus_avg"),
    )
)

artist_id_stats_2 = (
    outliers
    .groupBy("artist_id")
    .agg(F.percentile_approx("bpm", 0.5).alias("median_bpm"))
)

vs_group = (
    outliers.alias("s")
    .join(artist_id_stats_2.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") >= F.col("g.median_bpm"))
    .select(
        "s.*",
        "g.median_bpm",
        F.round(F.col("s.bpm") - F.col("g.median_bpm"), 2).alias("bpm_minus_median"),
    )
)

combined_details = (
    vs_group.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.moods",
        "s.bpm_vs_is_explicit_avg",
        "s.avg_bpm_vs_is_explicit_avg",
        "s.bpm_vs_is_explicit_avg_minus_avg",
        "s.bpm_minus_median",
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
    )
)

labeled = (
    combined_details
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        "bpm_vs_is_explicit_avg",
        "avg_bpm_vs_is_explicit_avg",
        "bpm_vs_is_explicit_avg_minus_avg",
        "bpm_minus_median",
        "artist_name",
        F.coalesce(F.col("bpm_minus_median"), F.lit(0.0)).alias("bpm_minus_median_filled"),
    )
)

with_parent_artist = (
    labeled.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.moods",
        "s.bpm_vs_is_explicit_avg",
        "s.avg_bpm_vs_is_explicit_avg",
        "s.bpm_vs_is_explicit_avg_minus_avg",
        "s.bpm_minus_median",
        "s.artist_name",
        "s.bpm_minus_median_filled",
        F.col("a.genre").alias("artist_genre"),
    )
)

result = (
    with_parent_artist
    .select(
        "id",
        "artist_id",
        "title",
        "moods",
        "bpm_vs_is_explicit_avg",
        "avg_bpm_vs_is_explicit_avg",
        "bpm_minus_median",
        "artist_name",
        "bpm_minus_median_filled",
    )
    .orderBy(
        F.col("avg_bpm_vs_is_explicit_avg").asc(),
        F.col("bpm_minus_median_filled").desc(),
        "id",
    )
    .limit(5)
)
