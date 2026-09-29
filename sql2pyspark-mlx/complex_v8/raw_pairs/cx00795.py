from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), F.col("id").desc())

scoped = (
    df_tracks
    .filter((F.col("artist_id") <= 3) | (F.length(F.trim(F.col("title"))) >= 7))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        F.least(F.col("duration_sec"), F.col("bpm")).alias("least_duration_sec_bpm"),
        F.abs(F.col("duration_sec") - 344).alias("duration_sec_distance"),
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

enriched = (
    scoped
    .filter(F.col("bpm") > 76)
    .select(
        "*",
        F.when(F.col("duration_sec_distance") >= 294, "HIGH").when(F.col("duration_sec_distance") >= 84, "MEDIUM").otherwise("LOW").alias("duration_sec_distance_tier"),
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
        F.concat(F.col("title_prefix"), F.lit("#"), F.col("duration_sec").cast("string")).alias("title_prefix_ref"),
    )
)

enriched_artist = (
    enriched.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select("s.*", "a.artist_name")
)

enriched_artist_2 = (
    enriched_artist.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.is_verified")), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "s.least_duration_sec_bpm",
        "s.duration_sec_distance",
        "s.title_prefix",
        "s.duration_sec_distance_tier",
        "s.moods_text",
        "s.title_prefix_ref",
        "s.artist_name",
        F.col("a.artist_name").alias("artist_artist_name"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

unique_rows = (
    enriched_artist_2
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

labeled = (
    unique_rows
    .select(
        "*",
        (F.floor(F.col("least_duration_sec_bpm") / 50) * 50).alias("least_duration_sec_bpm_bucket"),
        F.least(F.col("bpm"), F.col("duration_sec_distance")).alias("least_bpm_duration_sec_distance"),
        F.coalesce(F.col("duration_sec_distance"), F.lit(0)).alias("duration_sec_distance_filled"),
    )
)

result = (
    labeled
    .filter(~F.col("artist_is_verified"))
    .select(
        "id",
        "duration_sec",
        "bpm",
        "moods",
        "least_duration_sec_bpm",
        "duration_sec_distance",
        "title_prefix",
        "duration_sec_distance_tier",
        "moods_text",
        "title_prefix_ref",
        "artist_name",
        "artist_artist_name",
        "artist_is_verified",
        "duration_sec_distance_filled",
    )
    .orderBy(F.col("id").desc(), F.col("artist_artist_name").desc())
)
