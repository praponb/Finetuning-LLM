from pyspark.sql import functions as F

base = (
    df_tracks
    .filter((F.col("bpm") <= 127) & (F.col("duration_sec").between(154, 352)))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

with_details_artist = (
    base.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
    )
)

artist_country_stats = (
    with_details_artist
    .groupBy("artist_country")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

vs_group = (
    with_details_artist.alias("s")
    .join(artist_country_stats.alias("g"), F.col("g.artist_country") == F.col("s.artist_country"), "inner")
    .filter(F.col("s.duration_sec") > F.col("g.avg_duration_sec"))
    .select(
        "s.*",
        "g.avg_duration_sec",
        F.round(F.col("s.duration_sec") - F.col("g.avg_duration_sec"), 2).alias("duration_sec_minus_avg"),
    )
)

result = (
    vs_group
    .select("id", "avg_duration_sec", "duration_sec_minus_avg")
    .unpivot(
        ids=["id"],
        values=["avg_duration_sec", "duration_sec_minus_avg"],
        variableColumnName="field",
        valueColumnName="reading",
    )
)
