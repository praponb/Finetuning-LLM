from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_all_by_release_date_full = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date = Window.orderBy("release_date", "id")
w_duration_sec_tier_artist_id = Window.partitionBy("duration_sec_tier", "artist_id")
w_release_date_month_duration = Window.partitionBy(F.date_trunc("month", "release_date"), "duration_sec_tier").orderBy("duration_sec_filled", "id")
w_duration_sec_tier_artist_id_2 = Window.partitionBy("duration_sec_tier", "artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_tracks = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 2)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        F.when(F.col("duration_sec") >= 357, "large").when(F.col("duration_sec") >= 184, "medium").otherwise("small").alias("duration_sec_tier"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

filtered = (
    recent_tracks.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.device").isin("car", "phone", "speaker")), "left_anti")
)

with_parent_artist = (
    filtered.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre").isNotNull()), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.moods",
        "s.duration_sec_tier",
        "s.duration_sec_filled",
        "a.artist_name",
    )
)

ranked = (
    with_parent_artist
    .select(
        "*",
        F.size(F.collect_set("duration_sec_tier").over(w_artist_id)).alias("distinct_duration_sec_tier_in_artist_id"),
        F.max("duration_sec_filled").over(w_artist_id).alias("artist_id_max_duration_sec_filled"),
        F.first("duration_sec_filled").over(w_all_by_release_date_full).alias("first_duration_sec_filled"),
        F.lead("duration_sec_filled").over(w_all_by_release_date).alias("next_duration_sec_filled"),
        F.round(F.col("duration_sec_filled") - F.avg("duration_sec_filled").over(w_artist_id), 2).alias("duration_sec_filled_vs_artist_id_avg"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.sum("first_duration_sec_filled").over(w_duration_sec_tier_artist_id).alias("duration_sec_tier_artist_id_sum_first_duration_s"),
        F.max("rows_in_artist_id").over(w_duration_sec_tier_artist_id).alias("duration_sec_tier_artist_id_max_rows_in_artist_i"),
        F.lead("duration_sec_filled", 2).over(w_release_date_month_duration).alias("next2_duration_sec_filled"),
        F.nth_value("duration_sec_filled", 2).over(w_duration_sec_tier_artist_id_2).alias("second_duration_sec_filled"),
        F.round(F.col("first_duration_sec_filled") - F.avg("first_duration_sec_filled").over(w_duration_sec_tier_artist_id), 2).alias("first_duration_sec_filled_vs_duration_sec_tier_a"),
    )
)

flattened = (
    with_windows
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "duration_sec_tier",
        "duration_sec_filled",
        "artist_name",
        "distinct_duration_sec_tier_in_artist_id",
        "artist_id_max_duration_sec_filled",
        "first_duration_sec_filled",
        "next_duration_sec_filled",
        "duration_sec_filled_vs_artist_id_avg",
        "rows_in_artist_id",
        "duration_sec_tier_artist_id_sum_first_duration_s",
        "duration_sec_tier_artist_id_max_rows_in_artist_i",
        "next2_duration_sec_filled",
        "second_duration_sec_filled",
        "first_duration_sec_filled_vs_duration_sec_tier_a",
        F.explode("moods").alias("mood"),
    )
)

result = (
    flattened
    .select(
        "id",
        "artist_id",
        "artist_name",
        "distinct_duration_sec_tier_in_artist_id",
        "first_duration_sec_filled",
        "next_duration_sec_filled",
        "duration_sec_filled_vs_artist_id_avg",
        "rows_in_artist_id",
        "duration_sec_tier_artist_id_sum_first_duration_s",
        "next2_duration_sec_filled",
        "first_duration_sec_filled_vs_duration_sec_tier_a",
        "mood",
        F.month(F.col("release_date")).alias("release_date_month"),
        F.substring(F.trim(F.col("duration_sec_tier")), 1, 2).alias("duration_sec_tier_prefix"),
    )
    .orderBy(F.col("duration_sec_filled_vs_artist_id_avg").desc())
)
