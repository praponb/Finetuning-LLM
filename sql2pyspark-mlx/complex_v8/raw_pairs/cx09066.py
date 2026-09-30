from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre_by_duration_sec = Window.partitionBy("artist_genre").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w_is_explicit_flag = Window.partitionBy("is_explicit_flag")
w_artist_id_by_bpm_moving_2 = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(-2, Window.currentRow)
w_artist_genre_by_duration_sec_2 = Window.partitionBy("artist_genre").orderBy(F.col("duration_sec").desc_nulls_first())
w_artist_genre_by_duration_sec_3 = Window.partitionBy("artist_genre").orderBy(F.col("duration_sec").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_duration_sec_dense_rank_by_d = Window.partitionBy("duration_sec_dense_rank").orderBy("distinct_artist_ids", "duration_sec_dense_rank", "prev_duration_sec_tier", "distinct_artist_genre_in_is_explicit_flag").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_distinct_artist_ids_p = Window.orderBy(F.col("distinct_artist_ids").desc(), "duration_sec_dense_rank", "prev_duration_sec_tier", "distinct_artist_genre_in_is_explicit_flag").rowsBetween(-1, -1)
w_prev_duration_sec_tier_durat = Window.partitionBy("prev_duration_sec_tier", "duration_sec_dense_rank")

cleaned_tracks = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("N%"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
)

combined_details_artist = (
    cleaned_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.is_explicit_flag",
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
    )
)

sequenced = (
    combined_details_artist
    .select(
        "*",
        (F.col("is_explicit_flag") - F.lag("is_explicit_flag").over(w_artist_genre_by_duration_sec)).alias("is_explicit_flag_change"),
        F.size(F.collect_set("artist_genre").over(w_is_explicit_flag)).alias("distinct_artist_genre_in_is_explicit_flag"),
        F.round(F.avg("bpm").over(w_artist_id_by_bpm_moving_2), 2).alias("bpm_avg_3"),
        F.round(F.col("is_explicit_flag") / F.nullif(F.sum("is_explicit_flag").over(w_is_explicit_flag), F.lit(0)), 4).alias("is_explicit_flag_share_of_is_explicit_flag"),
        F.count(F.lit(1)).over(w_is_explicit_flag).alias("rows_in_is_explicit_flag"),
        F.dense_rank().over(w_artist_genre_by_duration_sec_2).alias("duration_sec_dense_rank"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_is_explicit_flag), F.lit(0)), 4).alias("duration_sec_share_of_is_explicit_flag"),
        F.round(F.cume_dist().over(w_artist_genre_by_duration_sec), 3).alias("duration_sec_cume_dist"),
        F.lag("duration_sec").over(w_artist_genre_by_duration_sec).alias("prev_duration_sec"),
        F.round(F.avg("duration_sec").over(w_artist_genre_by_duration_sec_3), 2).alias("duration_sec_avg_4"),
    )
)

totals = (
    sequenced
    .filter(F.col("duration_sec_share_of_is_explicit_flag") > 0.2)
    .groupBy(
        "duration_sec_dense_rank",
        F.when(F.col("prev_duration_sec") >= 312, "large").when(F.col("prev_duration_sec") >= 209, "medium").otherwise("small").alias("prev_duration_sec_tier"),
        "distinct_artist_genre_in_is_explicit_flag",
    )
    .agg(
        F.round(F.sum("duration_sec_dense_rank") / F.nullif(F.sum("duration_sec_cume_dist"), F.lit(0)), 3).alias("duration_sec_dense_rank_to_duration_sec_cume_dis"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
    )
)

with_windows = (
    totals
    .select(
        "*",
        F.last("distinct_artist_ids", ignorenulls=True).over(w_duration_sec_dense_rank_by_d).alias("last_known_distinct_artist_ids"),
        F.max("duration_sec_dense_rank").over(w_all_by_distinct_artist_ids_p).alias("duration_sec_dense_rank_max_1_prior"),
        F.round(F.col("distinct_artist_ids") - F.avg("distinct_artist_ids").over(w_prev_duration_sec_tier_durat), 2).alias("distinct_artist_ids_vs_prev_duration_sec_tier_du"),
        F.max("duration_sec_dense_rank").over(w_prev_duration_sec_tier_durat).alias("prev_duration_sec_tier_duration_sec_dense_rank_m"),
    )
)

result = (
    with_windows
    .filter(F.col("duration_sec_dense_rank_max_1_prior") < 20)
    .select(
        "duration_sec_dense_rank",
        "distinct_artist_genre_in_is_explicit_flag",
        "last_known_distinct_artist_ids",
        "duration_sec_dense_rank_max_1_prior",
        "distinct_artist_ids_vs_prev_duration_sec_tier_du",
        "prev_duration_sec_tier_duration_sec_dense_rank_m",
        F.greatest(F.col("distinct_artist_ids"), F.col("duration_sec_dense_rank")).alias("greatest_distinct_artist_ids_duration_sec_dense_"),
        F.lower(F.regexp_replace(F.trim(F.col("prev_duration_sec_tier")), " ", "_")).alias("prev_duration_sec_tier_slug"),
    )
    .orderBy("prev_duration_sec_tier_slug")
)
