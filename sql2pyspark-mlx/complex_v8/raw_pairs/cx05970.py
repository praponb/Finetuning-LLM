from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")
w_is_explicit_track_count_by_t = Window.partitionBy("is_explicit", "track_count").orderBy("track_is_explicit_count", "id")
w_artist_id_by_track_count = Window.partitionBy("artist_id").orderBy(F.col("track_count").desc(), "id")
w_artist_id_track_is_explicit = Window.partitionBy("artist_id", "track_is_explicit_count")
w_track_is_explicit_count_arti = Window.partitionBy("track_is_explicit_count", "artist_id").orderBy("track_count", "id")
w_track_is_explicit_count_arti_2 = Window.partitionBy("track_is_explicit_count", "artist_id").orderBy("track_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_is_explicit_count_arti_3 = Window.partitionBy("track_is_explicit_count", "artist_id").orderBy("track_count")

valid = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.array_contains(F.col("moods"), "party")))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count(F.when(F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
        F.count("*").alias("track_count"),
    )
)

with_counts = (
    valid.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

sequenced = (
    with_counts
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "moods",
        "track_is_explicit_count",
        "track_count",
        F.round(F.col("track_is_explicit_count") / F.nullif(F.sum("track_is_explicit_count").over(w_is_explicit), F.lit(0)), 4).alias("track_is_explicit_count_share_of_is_explicit"),
        F.lag("track_count", 3).over(w_is_explicit_track_count_by_t).alias("prev3_track_count"),
        F.round(F.col("track_count") / F.nullif(F.sum("track_count").over(w_is_explicit), F.lit(0)), 4).alias("track_count_share_of_is_explicit"),
        F.lag("track_count", 2).over(w_artist_id_by_track_count).alias("prev2_track_count"),
        (F.col("track_count") - F.lag("track_count").over(w_artist_id_by_track_count)).alias("track_count_change"),
        F.lead("track_is_explicit_count", 2).over(w_is_explicit_track_count_by_t).alias("next2_track_is_explicit_count"),
    )
)

windowed = (
    sequenced
    .select(
        "id",
        "artist_id",
        "title",
        "moods",
        "track_count",
        "prev3_track_count",
        "track_count_share_of_is_explicit",
        "prev2_track_count",
        "next2_track_is_explicit_count",
        F.round(F.col("track_count_change") - F.avg("track_count_change").over(w_artist_id_track_is_explicit), 2).alias("track_count_change_vs_artist_id_track_is_explici"),
        (F.col("track_count_change") - F.lag("track_count_change").over(w_track_is_explicit_count_arti)).alias("track_count_change_change"),
        F.max("prev2_track_count").over(w_track_is_explicit_count_arti_2).alias("max_prev2_track_count_so_far"),
        F.round(F.col("track_count_change") / F.nullif(F.sum("track_count_change").over(w_artist_id_track_is_explicit), F.lit(0)), 4).alias("track_count_change_share_of_artist_id_track_is_e"),
        F.round(F.cume_dist().over(w_track_is_explicit_count_arti_3), 3).alias("track_count_cume_dist"),
    )
)

labeled = (
    windowed
    .select(
        "id",
        "title",
        "moods",
        "track_count",
        "prev3_track_count",
        "track_count_share_of_is_explicit",
        "track_count_change_vs_artist_id_track_is_explici",
        "track_count_change_change",
        "track_count_cume_dist",
        F.round(F.col("track_count") / F.nullif(F.col("prev2_track_count"), F.lit(0)), 3).alias("track_count_per_prev2_track_count"),
    )
)

result = (
    labeled
    .filter(F.col("track_count_cume_dist") > 0.4)
    .select(
        "id",
        "moods",
        "track_count",
        "prev3_track_count",
        "track_count_share_of_is_explicit",
        "track_count_change_vs_artist_id_track_is_explici",
        "track_count_change_change",
        "track_count_cume_dist",
        "track_count_per_prev2_track_count",
        F.when(F.col("track_count_cume_dist") >= 0.6, "A").when(F.col("track_count_cume_dist") >= 0.2, "B").otherwise("C").alias("track_count_cume_dist_tier"),
        F.size(F.col("moods")).alias("moods_count"),
    )
    .orderBy("id")
    .limit(20)
)
