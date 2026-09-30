from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_is_explic = Window.partitionBy("release_date_month", "is_explicit").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_month = Window.partitionBy("release_date_month")
w_release_date_month_is_explic_2 = Window.partitionBy("release_date_month", "is_explicit").orderBy("release_date", "id").rowsBetween(-2, -1)
w_artist_id_release_date_month = Window.partitionBy("artist_id", "release_date_month").orderBy("duration_sec", "id")
w_release_date_month_is_explic_3 = Window.partitionBy("release_date_month", "is_explicit").orderBy("release_date", "id")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")

base = (
    df_tracks
    .filter((~F.col("is_explicit")) | (F.col("bpm") < 147))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_add(F.col("release_date"), 7).alias("release_date_plus_7d"),
        F.month(F.col("release_date")).alias("release_date_month"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "bpm",
        "moods",
        "release_date_plus_7d",
        "release_date_month",
        F.count(F.lit(1)).over(w_release_date_month_is_explic).alias("running_count"),
        F.size(F.collect_set("artist_id").over(w_release_date_month)).alias("distinct_artist_id_in_release_date_month"),
        F.max("release_date_month").over(w_release_date_month_is_explic_2).alias("release_date_month_max_2_prior"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_release_date_month)).alias("bpm_change"),
        (F.col("bpm") - F.lag("bpm").over(w_release_date_month_is_explic_3)).alias("bpm_change_2"),
        F.round(F.avg("bpm").over(w_release_date_month), 2).alias("release_date_month_avg_bpm"),
        F.min("release_date_month").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_min_release_date_month"),
        F.round(F.col("release_date_month") - F.avg("release_date_month").over(w_release_date_month), 2).alias("release_date_month_vs_release_date_month_avg"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "bpm",
        "release_date_plus_7d",
        "release_date_month",
        "running_count",
        "distinct_artist_id_in_release_date_month",
        "bpm_change",
        "bpm_change_2",
        "release_date_month_avg_bpm",
        "artist_id_is_explicit_min_release_date_month",
        "release_date_month_vs_release_date_month_avg",
        F.date_format(F.col("release_date_plus_7d"), "yyyy-MM").alias("release_date_plus_7d_label"),
    )
)
