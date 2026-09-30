from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy("release_date")
w_is_explicit = Window.partitionBy("is_explicit")
w_is_explicit_release_date_den = Window.partitionBy("is_explicit", "release_date_dense_rank")
w_release_date_dense_rank_is_e = Window.partitionBy("release_date_dense_rank", "is_explicit").orderBy(F.col("bpm").asc_nulls_last(), "id")
w_release_date_dense_rank_is_e_2 = Window.partitionBy("release_date_dense_rank", "is_explicit").orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_tracks = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

sequenced = (
    cleaned_tracks
    .select(
        "id",
        "is_explicit",
        "bpm",
        "moods",
        F.dense_rank().over(w_is_explicit_by_release_date).alias("release_date_dense_rank"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
    )
)

filtered = (
    sequenced.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (~F.col("p.is_skipped")) & (F.col("p.ms_played") > 166517), "left_semi")
)

windowed = (
    filtered
    .select(
        "id",
        "is_explicit",
        "bpm",
        "release_date_dense_rank",
        "rows_in_is_explicit",
        F.round(F.col("release_date_dense_rank") / F.nullif(F.sum("release_date_dense_rank").over(w_is_explicit_release_date_den), F.lit(0)), 4).alias("release_date_dense_rank_share_of_is_explicit_rel"),
        F.lag("rows_in_is_explicit", 1, 0).over(w_release_date_dense_rank_is_e).alias("prev_rows_in_is_explicit"),
        F.lead("bpm", 1, 0).over(w_release_date_dense_rank_is_e).alias("next_bpm"),
        F.sum("rows_in_is_explicit").over(w_release_date_dense_rank_is_e_2).alias("running_rows_in_is_explicit"),
    )
)

result = (
    windowed
    .select(
        "id",
        F.col("bpm").alias("bpm_v"),
        F.col("running_rows_in_is_explicit").alias("running_rows_in_is_explicit_v"),
    )
    .unpivot(
        ids=["id"],
        values=["bpm_v", "running_rows_in_is_explicit_v"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric_name")
)
