from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_bpm_running = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_all_by_bpm_per_duration_sec = Window.orderBy(F.col("bpm_per_duration_sec").desc(), "id")
w_artist_id_is_explicit_by_rel = Window.partitionBy("artist_id", "is_explicit").orderBy("release_date", "id").rowsBetween(-2, Window.currentRow)
w_artist_id_is_explicit_by_rel_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("release_date", "id")

scoped_tracks = (
    df_tracks
    .filter(F.col("duration_sec").between(154, 352))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

sequenced = (
    scoped_tracks
    .select(
        "*",
        F.first("bpm_per_duration_sec", ignorenulls=True).over(w_artist_id_by_bpm_running).alias("first_known_bpm_per_duration_sec"),
        F.max("bpm_per_duration_sec").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_max_bpm_per_duration_sec"),
        F.lead("release_date").over(w_all_by_bpm_per_duration_sec).alias("next_release_date"),
        F.lag("duration_sec", 3, 0).over(w_all_by_bpm_per_duration_sec).alias("prev3_duration_sec"),
        F.max("bpm").over(w_artist_id_is_explicit_by_rel).alias("bpm_max_3"),
        F.sum("bpm_per_duration_sec").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_sum_bpm_per_duration_sec"),
        F.row_number().over(w_artist_id_is_explicit_by_rel_2).alias("rn"),
        F.row_number().over(w_all_by_bpm_per_duration_sec).alias("bpm_per_duration_sec_seq"),
        (F.col("bpm_per_duration_sec") - F.lag("bpm_per_duration_sec").over(w_artist_id_is_explicit_by_rel_2)).alias("bpm_per_duration_sec_change"),
        F.max("duration_sec").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_max_duration_sec"),
    )
)

leaders = (
    sequenced
    .filter((F.col("rn") == 1) & (F.col("bpm_max_3").between(132, 516)))
    .drop("rn")
)

result = (
    leaders
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "bpm_per_duration_sec",
        "prev3_duration_sec",
        "is_explicit_artist_id_sum_bpm_per_duration_sec",
        "bpm_per_duration_sec_seq",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
    .orderBy("duration_sec", F.col("title").asc_nulls_last())
)
