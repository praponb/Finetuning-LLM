from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 1)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        F.abs(F.col("duration_sec") - 213).alias("duration_sec_distance"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
    )
)

enriched = (
    with_parent
    .filter(F.upper(F.trim(F.col("title"))).like("C%"))
    .select(
        "id",
        "artist_id",
        "title",
        "artist_country",
        F.add_months(F.col("release_date"), 3).alias("release_date_plus_3m"),
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
)

result = (
    enriched
    .filter(F.hour(F.col("release_date_month")).between(9, 14))
    .select(
        "id",
        "artist_id",
        "title",
        "artist_country",
        "release_date_plus_3m",
        F.date_add(F.col("release_date_plus_3m"), 7).alias("release_date_plus_3m_plus_7d"),
    )
    .orderBy(F.col("title").desc_nulls_first(), "release_date_plus_3m", "id")
    .limit(10)
)
