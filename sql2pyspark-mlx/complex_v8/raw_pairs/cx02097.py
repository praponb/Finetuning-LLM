from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_dayofweek_is_ex = Window.partitionBy("release_date_dayofweek", "is_explicit")
w_artist_id_by_bpm_distance_ru = Window.partitionBy("artist_id").orderBy("bpm_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_bpm_distance = Window.partitionBy("artist_id").orderBy("bpm_distance", "id")
w_all_by_distinct_artist_id_in = Window.orderBy("distinct_artist_id_in_release_date_dayofweek_is_", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_artist_id_in_releas = Window.partitionBy("distinct_artist_id_in_release_date_dayofweek_is_")

cleaned = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.sum("release_date_dayofweek").over(w_release_date_dayofweek_is_ex).alias("release_date_dayofweek_is_explicit_sum_release_d"),
        F.max("release_date_dayofweek").over(w_release_date_dayofweek_is_ex).alias("release_date_dayofweek_is_explicit_max_release_d"),
        F.size(F.collect_set("artist_id").over(w_release_date_dayofweek_is_ex)).alias("distinct_artist_id_in_release_date_dayofweek_is_"),
    )
)

filtered = (
    metrics.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

derived = (
    filtered
    .filter(F.col("release_date_dayofweek_is_explicit_max_release_d") > 18)
    .select(
        "id",
        "artist_id",
        "moods",
        "release_date_dayofweek",
        "release_date_dayofweek_is_explicit_sum_release_d",
        "release_date_dayofweek_is_explicit_max_release_d",
        "distinct_artist_id_in_release_date_dayofweek_is_",
        F.abs(F.col("bpm") - 100).alias("bpm_distance"),
        F.round(F.col("release_date_dayofweek") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("release_date_dayofweek_per_bpm"),
        (F.col("bpm") - F.col("release_date_dayofweek")).alias("bpm_minus_release_date_dayofweek"),
    )
)

with_details = (
    derived.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.moods",
        "s.distinct_artist_id_in_release_date_dayofweek_is_",
        "s.bpm_distance",
        "s.release_date_dayofweek_per_bpm",
        "a.artist_name",
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.last("bpm_distance", ignorenulls=True).over(w_artist_id_by_bpm_distance_ru).alias("last_known_bpm_distance"),
        (F.col("bpm_distance") - F.lag("bpm_distance").over(w_artist_id_by_bpm_distance)).alias("bpm_distance_change"),
        F.first("bpm_distance", ignorenulls=True).over(w_all_by_distinct_artist_id_in).alias("first_known_bpm_distance"),
        F.size(F.collect_set("artist_id").over(w_distinct_artist_id_in_releas)).alias("distinct_artist_id_in_distinct_artist_id_in_rele"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_id",
        "distinct_artist_id_in_release_date_dayofweek_is_",
        "bpm_distance",
        "release_date_dayofweek_per_bpm",
        "artist_name",
        "last_known_bpm_distance",
        "distinct_artist_id_in_distinct_artist_id_in_rele",
        F.concat(F.col("artist_name"), F.lit("#"), F.col("id").cast("string")).alias("artist_name_ref"),
    )
)
