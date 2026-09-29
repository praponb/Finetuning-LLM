from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_plus_7d = Window.orderBy(F.col("release_date_plus_7d").desc(), "id")
w_all_by_release_date_plus_7d_2 = Window.orderBy(F.col("release_date_plus_7d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_release_date_pl = Window.partitionBy("artist_id").orderBy("release_date_plus_7d", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy("release_date_plus_7d", "id")
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy("release_date_plus_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_release_date_pl_2 = Window.partitionBy("artist_id").orderBy("release_date_plus_7d")

filtered = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.col("bpm") <= 142))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "moods",
        F.when(F.col("duration_sec") >= 353, "A").when(F.col("duration_sec") >= 209, "B").otherwise("C").alias("duration_sec_tier"),
        F.date_add(F.col("release_date"), 7).alias("release_date_plus_7d"),
    )
)

ranked = (
    filtered
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_release_date_plus_7d), 3).alias("release_date_plus_7d_cume_dist"),
        F.last("release_date_plus_7d").over(w_all_by_release_date_plus_7d_2).alias("last_release_date_plus_7d"),
        F.last("release_date_plus_7d").over(w_artist_id_by_release_date_pl).alias("last_release_date_plus_7d_2"),
        F.round(F.percent_rank().over(w_is_explicit_by_release_date), 3).alias("release_date_plus_7d_percent_rank"),
        F.last("release_date_plus_7d", ignorenulls=True).over(w_is_explicit_by_release_date_2).alias("last_known_release_date_plus_7d"),
        F.first("release_date_plus_7d").over(w_all_by_release_date_plus_7d_2).alias("first_release_date_plus_7d"),
        F.row_number().over(w_all_by_release_date_plus_7d).alias("seq_no"),
        F.lead("release_date_plus_7d", 3).over(w_all_by_release_date_plus_7d).alias("next3_release_date_plus_7d"),
        F.ntile(10).over(w_is_explicit_by_release_date).alias("release_date_plus_7d_decile"),
        F.round(F.cume_dist().over(w_artist_id_by_release_date_pl_2), 3).alias("release_date_plus_7d_cume_dist_3"),
    )
)

top_per_group = ranked.filter(F.col("seq_no") <= 3)

result = (
    top_per_group
    .select(
        "id",
        "title",
        "is_explicit",
        "moods",
        "duration_sec_tier",
        "release_date_plus_7d",
        "release_date_plus_7d_cume_dist",
        "last_release_date_plus_7d_2",
        "last_known_release_date_plus_7d",
        "first_release_date_plus_7d",
        "next3_release_date_plus_7d",
        "release_date_plus_7d_decile",
        "release_date_plus_7d_cume_dist_3",
        F.add_months(F.col("release_date_plus_7d"), 1).alias("release_date_plus_7d_plus_1m"),
        F.lower(F.regexp_replace(F.trim(F.col("duration_sec_tier")), " ", "_")).alias("duration_sec_tier_slug"),
    )
)
