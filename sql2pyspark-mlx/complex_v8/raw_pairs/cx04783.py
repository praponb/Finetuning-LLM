from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("genre").orderBy(F.col("debut_year").desc(), F.col("id").desc())
w_dedup_2 = Window.partitionBy("genre", "country_or_unknown").orderBy(F.col("debut_year").desc(), F.col("id").desc())

base_artists = (
    df_artists
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.concat(F.col("artist_name"), F.lit("#"), F.col("debut_year").cast("string")).alias("artist_name_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("artist_name")), " ", "_")).alias("artist_name_slug"),
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)

latest_per_key = (
    base_artists
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

deduped = (
    latest_per_key
    .withColumn("row_num", F.row_number().over(w_dedup_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

derived = (
    deduped
    .filter(F.col("debut_year") > 1994)
    .select(
        "*",
        F.when(F.col("debut_year") >= 2016, "HIGH").when(F.col("debut_year") >= 1997, "MEDIUM").otherwise("LOW").alias("debut_year_tier"),
        F.concat(F.col("artist_name_ref"), F.lit("#"), F.col("debut_year").cast("string")).alias("artist_name_ref_ref"),
    )
)

line_level_tracks = (
    derived.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.length(F.trim(F.col("t.title"))) >= 8), "inner")
    .select(
        "s.id",
        "s.artist_name",
        "s.country",
        "s.debut_year",
        "s.is_verified",
        "s.artist_name_ref",
        "s.debut_year_tier",
        F.col("t.id").alias("track_id"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.title").alias("track_title"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

screened = (
    line_level_tracks
    .crossJoin(line_level_tracks.agg(F.avg("debut_year").alias("overall_avg_debut_year")))
    .filter(F.col("debut_year") > F.col("overall_avg_debut_year"))
    .drop("overall_avg_debut_year")
)

result = (
    screened
    .select("track_title", "is_verified", "id")
    .distinct()
)
