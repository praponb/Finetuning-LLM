from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy("duration_sec", "id")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w_artist_id_by_duration_sec_ru = Window.partitionBy("artist_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy("duration_sec", "id")
w_is_explicit_artist_id_by_bpm = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_duration_sec_3 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_artist_id_by_bpm_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm").desc(), "id")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")

recent = df_tracks.filter(F.col("bpm") <= 142)

sequenced = (
    recent
    .select(
        "id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_id_by_duration_sec)).alias("duration_sec_change"),
        F.round(F.cume_dist().over(w_is_explicit_by_duration_sec), 3).alias("duration_sec_cume_dist"),
        F.count(F.lit(1)).over(w_artist_id_by_duration_sec_ru).alias("running_count"),
        F.lag("release_date").over(w_is_explicit_by_duration_sec_2).alias("prev_release_date"),
        F.last("bpm").over(w_is_explicit_artist_id_by_bpm).alias("last_bpm"),
        F.last("bpm", ignorenulls=True).over(w_is_explicit_by_duration_sec_3).alias("last_known_bpm"),
        F.lead("duration_sec", 3, 0).over(w_is_explicit_artist_id_by_bpm_2).alias("next3_duration_sec"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_is_explicit), 2).alias("duration_sec_vs_artist_id_is_explicit_avg"),
    )
)

enriched = sequenced.withColumn("title_len", F.length(F.trim(F.col("title"))))

result = (
    enriched
    .select(
        "id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "duration_sec_change",
        "running_count",
        "prev_release_date",
        "last_bpm",
        "last_known_bpm",
        "next3_duration_sec",
        "duration_sec_vs_artist_id_is_explicit_avg",
        "title_len",
    )
    .orderBy(F.col("bpm").desc(), "id")
    .limit(5)
)
