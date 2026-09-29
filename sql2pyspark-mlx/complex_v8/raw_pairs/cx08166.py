from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_artist_id = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_id").orderBy("bpm_per_duration_sec", "id")
w_release_date_month_artist_id_2 = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_id").orderBy("bpm_per_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

sequenced = (
    recent
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        "title_slug",
        "title_len",
        "bpm_per_duration_sec",
        F.row_number().over(w_release_date_month_artist_id).alias("rn"),
        F.first("bpm_per_duration_sec", ignorenulls=True).over(w_release_date_month_artist_id_2).alias("first_known_bpm_per_duration_sec"),
    )
)

flattened = (
    sequenced
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "title_slug",
        "title_len",
        "bpm_per_duration_sec",
        "rn",
        "first_known_bpm_per_duration_sec",
        F.explode_outer("moods").alias("mood"),
    )
)

result = (
    flattened
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "title_slug",
        "title_len",
        "bpm_per_duration_sec",
        "rn",
        "first_known_bpm_per_duration_sec",
        "mood",
        F.round(F.col("duration_sec") / F.nullif(F.col("title_len"), F.lit(0)), 3).alias("duration_sec_per_title_len"),
        F.concat(F.col("title_slug"), F.lit("#"), F.col("duration_sec").cast("string")).alias("title_slug_ref"),
    )
    .orderBy(F.col("id").asc_nulls_last(), "first_known_bpm_per_duration_sec", "mood")
    .limit(20)
)
