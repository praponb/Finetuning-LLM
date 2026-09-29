from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moods_count_by_bpm_per_durat = Window.partitionBy("moods_count").orderBy("bpm_per_duration_sec", "id")
w_moods_count_by_bpm_per_durat_2 = Window.partitionBy("moods_count").orderBy("bpm_per_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moods_count_by_bpm_per_durat_3 = Window.partitionBy("moods_count").orderBy("bpm_per_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tracks = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 1) & (F.col("release_date") >= F.lit("2024-11-06").cast("date")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.size(F.col("moods")).alias("moods_count"),
        F.add_months(F.col("release_date"), 1).alias("release_date_plus_1m"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

with_trends = (
    recent_tracks
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "bpm_per_duration_sec",
        F.row_number().over(w_moods_count_by_bpm_per_durat).alias("rn"),
        F.nth_value("release_date", 2).over(w_moods_count_by_bpm_per_durat_2).alias("second_release_date"),
        F.sum("duration_sec").over(w_moods_count_by_bpm_per_durat_3).alias("running_duration_sec"),
    )
)

result = (
    with_trends
    .select("id", "bpm", "running_duration_sec")
    .unpivot(
        ids=["id"],
        values=["bpm", "running_duration_sec"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
