from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_country", "artist_genre").orderBy("bpm", "id")
w_dedup_2 = Window.partitionBy("artist_id").orderBy("bpm", "id")

scoped = (
    df_tracks
    .filter((F.col("duration_sec") > 209) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

enriched = (
    scoped.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "s.title_ref",
        "s.title_slug",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
    )
)

enriched_artist = (
    enriched.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.title_ref",
        "s.artist_country",
        "s.artist_genre",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.genre").alias("artist_genre_2"),
    )
)

deduped = (
    enriched_artist
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

unique_rows = (
    deduped
    .withColumn("recency_rank", F.row_number().over(w_dedup_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

screened = (
    unique_rows.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.country").isNotNull()), "left_anti")
)

with_details_artist = (
    screened.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.title_ref",
        "s.artist_genre",
        "s.artist_genre_2",
        F.col("a.debut_year").alias("artist_debut_year_2"),
    )
)

result = (
    with_details_artist
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "title_ref",
        "artist_genre",
        "artist_genre_2",
        "artist_debut_year_2",
        F.when(F.col("artist_genre").isin("HipHop", "Classical"), "group_a").when(F.col("artist_genre") == "Pop", "group_b").otherwise("other").alias("artist_genre_group"),
    )
    .orderBy(F.col("title").desc())
)
