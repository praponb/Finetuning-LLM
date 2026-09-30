from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_is_explicit_release_date_mon = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date")).orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_release_date_mon_2 = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date")).orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_release_date_mon_3 = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date")).orderBy(F.col("bpm").desc(), "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 1) & (F.col("artist_id") >= 2))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
    )
)

sequenced = (
    valid
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "moods_text",
        F.round(F.percent_rank().over(w_is_explicit_artist_id_by_dur), 3).alias("duration_sec_percent_rank"),
        F.nth_value("duration_sec", 2).over(w_is_explicit_release_date_mon).alias("second_duration_sec"),
        F.first("duration_sec", ignorenulls=True).over(w_is_explicit_release_date_mon_2).alias("first_known_duration_sec"),
        F.count(F.lit(1)).over(w_is_explicit_release_date_mon_2).alias("running_count"),
        F.max("duration_sec").over(w_artist_id).alias("artist_id_max_duration_sec"),
        F.sum("duration_sec").over(w_is_explicit_release_date_mon_3).alias("duration_sec_sum_7"),
    )
)

per_label = (
    sequenced
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods_text",
        "duration_sec_percent_rank",
        "second_duration_sec",
        "first_known_duration_sec",
        "running_count",
        "artist_id_max_duration_sec",
        "duration_sec_sum_7",
        F.explode("moods").alias("mood"),
    )
)

scored = (
    per_label
    .select(
        "*",
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
        F.substring(F.trim(F.col("mood")), 1, 3).alias("mood_prefix"),
    )
)

result = (
    scored
    .cube("artist_id", "mood", "is_explicit")
    .agg(F.array_sort(F.collect_set("title")).alias("title_list"))
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("mood").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)
