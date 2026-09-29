from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_artist_id = Window.partitionBy(F.month("release_date"), "artist_id")
w_all_by_bpm = Window.orderBy("bpm", "id")
w_all_by_bpm_moving_2 = Window.orderBy("bpm", "id").rowsBetween(-2, Window.currentRow)

valid_tracks = (
    df_tracks
    .filter((F.col("is_explicit")) & (F.col("release_date") < F.lit("2025-08-07").cast("date")))
)

sequenced = (
    valid_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.max("duration_sec").over(w_release_date_month_artist_id).alias("release_date_month_artist_id_max_duration_sec"),
        F.ntile(4).over(w_all_by_bpm).alias("bpm_quartile"),
        F.lead("duration_sec", 3).over(w_all_by_bpm).alias("next3_duration_sec"),
        F.rank().over(w_all_by_bpm).alias("bpm_rank"),
        F.round(F.avg("duration_sec").over(w_all_by_bpm_moving_2), 2).alias("duration_sec_avg_3"),
    )
)

joined = (
    sequenced.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.upper(F.trim(F.col("a.artist_name"))).like("K%"))
    .select("s.*", "a.artist_name", F.col("a.genre").alias("artist_genre"))
)

result = (
    joined
    .select(
        "id",
        "bpm_rank",
        "bpm",
        "release_date_month_artist_id_max_duration_sec",
        "bpm_quartile",
    )
    .unpivot(
        ids=["id"],
        values=["bpm_rank", "bpm", "release_date_month_artist_id_max_duration_sec", "bpm_quartile"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
