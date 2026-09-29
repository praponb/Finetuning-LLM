from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_running = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_all_by_release_date_running_2 = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tracks = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 1) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

ranked = (
    scoped_tracks
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        F.count(F.lit(1)).over(w_all_by_release_date_running).alias("running_count"),
        F.round(F.avg("bpm").over(w_artist_id), 2).alias("artist_id_avg_bpm"),
        F.last("bpm", ignorenulls=True).over(w_all_by_release_date_running).alias("last_known_bpm"),
        F.first("release_date", ignorenulls=True).over(w_all_by_release_date_running_2).alias("first_known_release_date"),
        F.sum("bpm").over(w_artist_id).alias("artist_id_sum_bpm"),
    )
)

result = (
    ranked
    .filter(F.col("artist_id_sum_bpm") < 1191)
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "last_known_bpm",
        "first_known_release_date",
        "artist_id_sum_bpm",
        F.date_trunc("week", F.col("first_known_release_date")).alias("first_known_release_date_week"),
        F.when(F.col("running_count") >= 13, "A").when(F.col("running_count") >= 10, "B").otherwise("C").alias("running_count_tier"),
    )
    .orderBy(F.col("first_known_release_date_week").desc_nulls_first(), "id")
)
