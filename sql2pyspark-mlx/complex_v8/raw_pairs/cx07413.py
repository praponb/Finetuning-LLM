from pyspark.sql import functions as F

scoped = (
    df_tracks
    .filter((~F.col("is_explicit")) | (F.col("duration_sec").between(142, 352)))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
    )
)

with_details_artist = (
    scoped.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "s.release_date_label",
        "s.title_clean",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_details_artist_2 = (
    with_details_artist.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.moods",
        "s.title_clean",
        "s.artist_debut_year",
        F.col("a.debut_year").alias("artist_debut_year_2"),
        "a.artist_name",
    )
)

summary = (
    with_details_artist_2
    .groupBy(
        F.date_trunc("month", "release_date").alias("release_date_month"),
        "artist_id",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.max("release_date").alias("last_release_date"),
    )
)

scored = (
    summary
    .filter(F.col("n_rows") <= 8)
    .select(
        "release_date_month",
        "artist_id",
        "n_rows",
        F.abs(F.col("n_rows") - 7).alias("n_rows_distance"),
        F.to_date(F.col("release_date_month")).alias("release_date_month_date"),
    )
)

with_parent_artist = (
    scored.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.release_date_month",
        "s.artist_id",
        "s.n_rows",
        "s.n_rows_distance",
        F.col("a.is_verified").alias("artist_is_verified"),
        "a.artist_name",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    with_parent_artist
    .select(
        "release_date_month",
        "artist_id",
        "n_rows",
        "artist_is_verified",
        (F.col("n_rows") - F.col("artist_debut_year")).alias("n_rows_minus_artist_debut_year"),
    )
    .orderBy(
        F.col("artist_id").asc(),
        F.col("n_rows").desc_nulls_first(),
        "release_date_month",
    )
    .limit(10)
)
