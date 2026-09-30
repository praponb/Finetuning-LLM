from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy("bpm", "id")
w_artist_id_by_bpm_2 = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id")
w_artist_id_by_bpm_ties = Window.partitionBy("artist_id").orderBy("bpm")
w_all_by_bpm = Window.orderBy(F.col("bpm").desc(), "id")
w_artist_id_by_bpm_ties_2 = Window.partitionBy("artist_id").orderBy(F.col("bpm").asc_nulls_last())
w_rn = Window.partitionBy("rn")
w_all_by_next_bpm_2_ties = Window.orderBy(F.col("next_bpm_2").asc_nulls_last())
w_artist_id_by_rn_ties = Window.partitionBy("artist_id").orderBy(F.col("rn").desc())

filtered = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "moods",
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "artist_id",
        "moods",
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.lag("bpm", 3).over(w_artist_id_by_bpm).alias("prev3_bpm"),
        F.lead("bpm", 1, 0).over(w_artist_id_by_bpm_2).alias("next_bpm"),
        F.lead("bpm").over(w_artist_id_by_bpm_2).alias("next_bpm_2"),
        F.rank().over(w_artist_id_by_bpm_ties).alias("bpm_rank"),
        F.min("bpm").over(w_artist_id).alias("artist_id_min_bpm"),
        F.row_number().over(w_all_by_bpm).alias("rn"),
        F.lead("bpm", 2).over(w_artist_id_by_bpm).alias("next2_bpm"),
        F.round(F.percent_rank().over(w_artist_id_by_bpm_ties_2), 3).alias("bpm_percent_rank"),
    )
)

with_windows = (
    sequenced
    .select(
        "id",
        "artist_id",
        "moods",
        "bpm_vs_artist_id_avg",
        "bpm_share_of_artist_id",
        "next_bpm",
        "next_bpm_2",
        "bpm_rank",
        "rn",
        "bpm_percent_rank",
        F.min("rn").over(w_rn).alias("rn_min_rn"),
        F.round(F.percent_rank().over(w_all_by_next_bpm_2_ties), 3).alias("next_bpm_2_percent_rank"),
        F.round(F.percent_rank().over(w_artist_id_by_rn_ties), 3).alias("rn_percent_rank"),
        F.round(F.avg("next_bpm").over(w_rn), 2).alias("rn_avg_next_bpm"),
    )
)

stats = (
    with_windows
    .filter(F.array_contains(F.col("moods"), "focus"))
    .groupBy("artist_id", "rn")
    .agg(
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("next_bpm_2") / F.nullif(F.sum("bpm_vs_artist_id_avg"), F.lit(0)), 3).alias("next_bpm_2_to_bpm_vs_artist_id_avg_ratio"),
        F.sum("bpm_percent_rank").alias("total_bpm_percent_rank"),
    )
)

result = (
    stats
    .select(
        "artist_id",
        "distinct_ids",
        "next_bpm_2_to_bpm_vs_artist_id_avg_ratio",
        "total_bpm_percent_rank",
        F.round(F.col("distinct_ids") / F.nullif(F.col("total_bpm_percent_rank"), F.lit(0)), 3).alias("distinct_ids_per_total_bpm_percent_rank"),
    )
    .orderBy(F.col("next_bpm_2_to_bpm_vs_artist_id_avg_ratio").desc())
)
