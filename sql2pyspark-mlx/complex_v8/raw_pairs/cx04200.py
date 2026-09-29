from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), F.col("id").desc())

prepared = (
    df_tracks
    .filter((F.col("is_explicit")) & (F.col("title").isNotNull()))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.date_add(F.col("release_date"), 30).alias("release_date_plus_30d"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
    )
)

unique_rows = (
    prepared
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_details_artist = (
    unique_rows.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
    )
)

result = (
    with_details_artist
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "release_date_plus_30d",
        "release_date_label",
        "artist_is_verified",
        "artist_country",
        "artist_name",
        F.concat_ws("|", F.col("release_date_label"), F.col("artist_country")).alias("release_date_label_artist_country_label"),
    )
    .orderBy(F.col("artist_name").asc_nulls_last(), "id")
    .limit(10)
)
