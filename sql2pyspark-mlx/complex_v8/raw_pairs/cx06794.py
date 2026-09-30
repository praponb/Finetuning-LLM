from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")

scoped = df_tracks.filter((F.col("bpm") >= 75) & (F.lower(F.col("title")).like("%ne%")))

with_windows = (
    scoped
    .withColumn("duration_sec_share_of_is_explicit_artist_id", F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_is_explicit_artist_id), F.lit(0)), 4))
    .withColumn("rows_in_is_explicit_artist_id", F.count(F.lit(1)).over(w_is_explicit_artist_id))
)

result = (
    with_windows
    .select(
        "id",
        "bpm",
        "rows_in_is_explicit_artist_id",
        "duration_sec",
    )
    .unpivot(
        ids=["id"],
        values=["bpm", "rows_in_is_explicit_artist_id", "duration_sec"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
