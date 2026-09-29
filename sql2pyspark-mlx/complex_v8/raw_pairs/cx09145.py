from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id")
w_is_explicit_by_duration_sec_3 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-1, Window.currentRow)

valid = (
    df_tracks
    .filter((F.col("bpm") <= 163) & (F.col("duration_sec") < 365))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
)

with_trends = (
    valid
    .select(
        "*",
        F.min("duration_sec").over(w_artist_id).alias("artist_id_min_duration_sec"),
        F.min("duration_sec").over(w_is_explicit_by_duration_sec).alias("min_duration_sec_so_far"),
        F.lead("release_date_month", 2).over(w_is_explicit_by_duration_sec_2).alias("next2_release_date_month"),
        F.round(F.avg("duration_sec").over(w_is_explicit_by_duration_sec_3), 2).alias("duration_sec_avg_2"),
    )
)

result = (
    with_trends
    .select("id", "duration_sec", "artist_id_min_duration_sec")
    .unpivot(
        ids=["id"],
        values=["duration_sec", "artist_id_min_duration_sec"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric")
)
