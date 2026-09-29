from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_duration_sec_tier_artist_id = Window.partitionBy("duration_sec_tier", "artist_id")
w_duration_sec_tier_by_duratio = Window.partitionBy("duration_sec_tier").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit = Window.partitionBy("is_explicit")
w_is_explicit_artist_id_by_bpm = Window.partitionBy("is_explicit", "artist_id").orderBy("bpm", "id")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy("release_date", "id")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id")
w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy("release_date", "id")
w_duration_sec_tier_by_duratio_2 = Window.partitionBy("duration_sec_tier").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("duration_sec") >= 352, "HIGH").when(F.col("duration_sec") >= 165, "MEDIUM").otherwise("LOW").alias("duration_sec_tier"),
    )
)

joined = (
    valid.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select("s.*", "a.artist_name")
)

metrics = (
    joined
    .select(
        "*",
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_duration_sec_tier_artist_id), F.lit(0)), 4).alias("bpm_share_of_duration_sec_tier_artist_id"),
        F.nth_value("bpm", 2).over(w_duration_sec_tier_by_duratio).alias("second_bpm"),
        F.count(F.lit(1)).over(w_duration_sec_tier_artist_id).alias("rows_in_duration_sec_tier_artist_id"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit), F.lit(0)), 4).alias("bpm_share_of_is_explicit"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_is_explicit_artist_id_by_bpm)).alias("days_since_prev_release_date"),
        F.size(F.collect_set("duration_sec_tier").over(w_is_explicit)).alias("distinct_duration_sec_tier_in_is_explicit"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_duration_sec_tier_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_duration_sec_tier_artist_i"),
        F.ntile(10).over(w_is_explicit_by_release_date).alias("release_date_decile"),
        F.lead("release_date", 3).over(w_is_explicit_by_duration_sec).alias("next3_release_date"),
        F.lead("duration_sec").over(w_is_explicit_artist_id_by_rel).alias("next_duration_sec"),
        F.last("release_date", ignorenulls=True).over(w_duration_sec_tier_by_duratio_2).alias("last_known_release_date"),
    )
)

derived = (
    metrics
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "duration_sec_tier",
        "artist_name",
        "bpm_share_of_duration_sec_tier_artist_id",
        "second_bpm",
        "bpm_share_of_is_explicit",
        "days_since_prev_release_date",
        "distinct_duration_sec_tier_in_is_explicit",
        "duration_sec_share_of_duration_sec_tier_artist_i",
        "release_date_decile",
        "next_duration_sec",
        "last_known_release_date",
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        F.round(F.col("bpm_share_of_is_explicit") * 1.5, 2).alias("bpm_share_of_is_explicit_adj"),
    )
)

result = (
    derived
    .select(
        "id",
        "artist_id",
        "release_date",
        "artist_name",
        "second_bpm",
        "bpm_share_of_is_explicit",
        "days_since_prev_release_date",
        "distinct_duration_sec_tier_in_is_explicit",
        "duration_sec_share_of_duration_sec_tier_artist_i",
        "release_date_decile",
        "next_duration_sec",
        "last_known_release_date",
        "duration_sec_bucket",
        "bpm_share_of_is_explicit_adj",
        F.round(F.col("bpm_share_of_is_explicit") * 0.9, 2).alias("bpm_share_of_is_explicit_adj_2"),
    )
    .orderBy(F.col("bpm_share_of_is_explicit_adj_2").asc_nulls_last(), "id")
)
