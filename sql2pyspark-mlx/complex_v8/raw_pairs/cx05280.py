from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("avg_duration_sec_adj").desc(), F.col("id").desc())

valid = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2025-08-02").cast("date")) | (F.col("duration_sec") > 213))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
    )
)

with_parent_artist = (
    valid.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

artist_id_stats = (
    with_parent_artist
    .groupBy("artist_id")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

vs_group = (
    with_parent_artist.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.duration_sec") > F.col("g.avg_duration_sec"))
    .select(
        "s.*",
        "g.avg_duration_sec",
        F.round(F.col("s.duration_sec") - F.col("g.avg_duration_sec"), 2).alias("duration_sec_minus_avg"),
    )
)

scored = (
    vs_group
    .filter(F.size(F.col("moods")) >= 2)
    .select(
        "*",
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
        F.round(F.col("avg_duration_sec") * 1.5, 2).alias("avg_duration_sec_adj"),
        F.least(F.col("artist_debut_year"), F.col("bpm")).alias("least_artist_debut_year_bpm"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "artist_is_verified",
        "avg_duration_sec",
        "bpm_filled",
        "avg_duration_sec_adj",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.round(F.col("duration_sec_minus_avg") * 0.9, 2).alias("duration_sec_minus_avg_adj"),
    )
)
