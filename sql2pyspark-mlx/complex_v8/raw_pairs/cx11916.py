from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_title_len = Window.partitionBy("artist_id").orderBy(F.col("title_len").desc(), "id")
w_artist_id = Window.partitionBy("artist_id")
w_title_len_artist_genre_by_ti = Window.partitionBy("title_len", "artist_genre").orderBy("title_len", "id")
w_title_len_artist_genre_by_ti_2 = Window.partitionBy("title_len", "artist_genre").orderBy("title_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_title_len_artist_genre_by_ti_3 = Window.partitionBy("title_len", "artist_genre").orderBy("title_len")
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_all_by_duration_sec_ties = Window.orderBy(F.col("duration_sec").desc())
w_all_by_title_len = Window.orderBy(F.col("title_len").desc(), "id")
w_artist_id_by_title_len_ties = Window.partitionBy("artist_id").orderBy(F.col("title_len").desc())
w_all_by_title_len_prior_1 = Window.orderBy(F.col("title_len").desc(), "id").rowsBetween(-1, -1)
w_title_len_percent_rank_tier = Window.partitionBy("title_len_percent_rank_tier", "title_len_dense_rank")
w_title_len_dense_rank_duratio = Window.partitionBy("title_len_dense_rank", "duration_sec_dense_rank").orderBy(F.col("seq_no").desc_nulls_first(), "id")
w_duration_sec_dense_rank_next = Window.partitionBy("duration_sec_dense_rank", "next2_release_date_year")
w_distinct_title_len_percent_r = Window.partitionBy("distinct_title_len_percent_rank_tiers").orderBy("distinct_title_len_percent_rank_tiers", "next2_release_date_year", "duration_sec_dense_rank").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_distinct_title_len_pe = Window.orderBy("distinct_title_len_percent_rank_tiers", "next2_release_date_year", "duration_sec_dense_rank").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 1)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)

with_details_artist = (
    cleaned.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "s.title_len",
        F.col("a.genre").alias("artist_genre"),
    )
)

with_windows = (
    with_details_artist
    .select(
        "*",
        F.row_number().over(w_artist_id_by_title_len).alias("seq_no"),
        F.sum("title_len").over(w_artist_id).alias("artist_id_sum_title_len"),
        F.lead("release_date", 2).over(w_title_len_artist_genre_by_ti).alias("next2_release_date"),
        F.first("duration_sec", ignorenulls=True).over(w_title_len_artist_genre_by_ti_2).alias("first_known_duration_sec"),
        F.rank().over(w_title_len_artist_genre_by_ti_3).alias("title_len_rank"),
        F.lead("duration_sec").over(w_all_by_duration_sec).alias("next_duration_sec"),
        F.dense_rank().over(w_all_by_duration_sec_ties).alias("duration_sec_dense_rank"),
        F.round(F.avg("title_len").over(w_artist_id), 2).alias("artist_id_avg_title_len"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_title_len_artist_genre_by_ti)).alias("days_since_prev_release_date"),
        F.ntile(5).over(w_all_by_title_len).alias("title_len_quintile"),
        F.dense_rank().over(w_artist_id_by_title_len_ties).alias("title_len_dense_rank"),
        F.max("title_len").over(w_all_by_title_len_prior_1).alias("title_len_max_1_prior"),
        F.round(F.percent_rank().over(w_all_by_title_len), 3).alias("title_len_percent_rank"),
    )
)

enriched = (
    with_windows
    .filter(F.col("title_len_quintile").between(1, 9))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "artist_genre",
        "seq_no",
        "artist_id_sum_title_len",
        "next2_release_date",
        "title_len_rank",
        "next_duration_sec",
        "duration_sec_dense_rank",
        "days_since_prev_release_date",
        "title_len_dense_rank",
        F.when(F.col("title_len_percent_rank") >= 0.6, "large").when(F.col("title_len_percent_rank") >= 0.3, "medium").otherwise("small").alias("title_len_percent_rank_tier"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        (F.col("title_len_max_1_prior") - F.col("title_len_quintile")).alias("title_len_max_1_prior_minus_title_len_quintile"),
    )
)

with_trends = (
    enriched
    .select(
        "id",
        "title",
        "duration_sec",
        "artist_genre",
        "next2_release_date",
        "next_duration_sec",
        "duration_sec_dense_rank",
        "days_since_prev_release_date",
        "title_len_percent_rank_tier",
        "title_len_max_1_prior_minus_title_len_quintile",
        F.count(F.lit(1)).over(w_title_len_percent_rank_tier).alias("rows_in_title_len_percent_rank_tier_title_len_de"),
        F.row_number().over(w_title_len_dense_rank_duratio).alias("seq_no_2"),
    )
)

summary = (
    with_trends
    .groupBy(
        F.year("next2_release_date").alias("next2_release_date_year"),
        "duration_sec_dense_rank",
    )
    .agg(
        F.count(F.when(F.upper(F.trim(F.col("title"))).like("S%"), 1)).alias("n_upper_trim_title_like_s"),
        F.countDistinct("title_len_percent_rank_tier").alias("distinct_title_len_percent_rank_tiers"),
        F.sum("seq_no_2").alias("sum_seq_no_2_check"),
    )
    .filter(F.col("sum_seq_no_2_check") > 3)
    .drop("sum_seq_no_2_check")
)

with_windows_2 = (
    summary
    .select(
        "*",
        F.count(F.lit(1)).over(w_duration_sec_dense_rank_next).alias("rows_in_duration_sec_dense_rank_next2_release_da"),
        F.round(F.col("duration_sec_dense_rank") / F.nullif(F.sum("duration_sec_dense_rank").over(w_duration_sec_dense_rank_next), F.lit(0)), 4).alias("duration_sec_dense_rank_share_of_duration_sec_de"),
        F.count(F.lit(1)).over(w_distinct_title_len_percent_r).alias("running_count"),
        F.last("next2_release_date_year", ignorenulls=True).over(w_all_by_distinct_title_len_pe).alias("last_known_next2_release_date_year"),
    )
)

result = (
    with_windows_2
    .filter(F.col("running_count") < 12)
    .select(
        "next2_release_date_year",
        "n_upper_trim_title_like_s",
        "distinct_title_len_percent_rank_tiers",
        "rows_in_duration_sec_dense_rank_next2_release_da",
        "duration_sec_dense_rank_share_of_duration_sec_de",
        "running_count",
    )
    .orderBy(F.col("distinct_title_len_percent_rank_tiers").asc_nulls_last())
)
