from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_bpm_running = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy("release_date")
w_all_by_bpm = Window.orderBy("bpm", "id")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_all_by_release_date = Window.orderBy("release_date", "id")
w_is_explicit_by_release_date_3 = Window.partitionBy("is_explicit").orderBy("release_date", "id")

base = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("P%")) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
    )
)

ranked = (
    base
    .select(
        "*",
        F.max("duration_sec").over(w_is_explicit_by_bpm_running).alias("max_duration_sec_so_far"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.first("release_date", ignorenulls=True).over(w_is_explicit_by_release_date).alias("first_known_release_date"),
        F.rank().over(w_is_explicit_by_release_date_2).alias("release_date_rank"),
        F.ntile(4).over(w_all_by_bpm).alias("bpm_quartile"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("bpm_share_of_artist_id_is_explicit"),
        F.round(F.percent_rank().over(w_all_by_release_date), 3).alias("release_date_percent_rank"),
        F.round(F.avg("bpm").over(w_artist_id), 2).alias("artist_id_avg_bpm"),
        F.lag("bpm", 1, 0).over(w_is_explicit_by_release_date_3).alias("prev_bpm"),
    )
)

top_rows = (
    ranked
    .filter((F.col("release_date_rank") <= 2) & (F.array_contains(F.col("moods"), "party")))
    .drop("release_date_rank")
)

result = (
    top_rows
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "title_prefix",
        "bpm_share_of_artist_id",
        "first_known_release_date",
        "bpm_vs_artist_id_avg",
        "release_date_percent_rank",
        "artist_id_avg_bpm",
        "prev_bpm",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
    .orderBy("is_explicit_label")
)
