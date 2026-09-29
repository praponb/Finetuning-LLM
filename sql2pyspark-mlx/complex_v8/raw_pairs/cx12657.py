from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_title_len_by_release_date_pr = Window.partitionBy("title_len").orderBy("release_date", "id").rowsBetween(-1, -1)
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_by_duration_sec_pe = Window.partitionBy("artist_id").orderBy("duration_sec_per_bpm", "id")
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.date_trunc("month", "release_date")).orderBy("duration_sec_per_bpm", "id")
w_title_len_by_release_date = Window.partitionBy("title_len").orderBy("release_date", "id")
w_title_len_by_release_date_ti = Window.partitionBy("title_len").orderBy("release_date")
w_release_date_year_duration_s = Window.partitionBy(F.year("release_date"), "duration_sec_per_bpm_seq")
w_release_date_decile_by_days = Window.partitionBy("release_date_decile").orderBy(F.col("days_since_prev_release_date").desc(), "id")
w_release_date_decile_by_days_2 = Window.partitionBy("release_date_decile").orderBy(F.col("days_since_prev_release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tracks = (
    df_tracks
    .filter((F.col("bpm") > 85) & (F.col("artist_id") <= 14))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "moods",
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

metrics = (
    filtered_tracks
    .select(
        "*",
        F.round(F.avg("duration_sec_per_bpm").over(w_title_len_by_release_date_pr), 2).alias("duration_sec_per_bpm_avg_1_prior"),
        F.round(F.col("title_len") / F.nullif(F.sum("title_len").over(w_is_explicit), F.lit(0)), 4).alias("title_len_share_of_is_explicit"),
        F.row_number().over(w_artist_id_by_duration_sec_pe).alias("rn"),
        F.row_number().over(w_artist_id_release_date_month).alias("duration_sec_per_bpm_seq"),
        F.ntile(10).over(w_title_len_by_release_date).alias("release_date_decile"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_artist_id_by_duration_sec_pe)).alias("days_since_prev_release_date"),
        F.lead("title_len").over(w_title_len_by_release_date).alias("next_title_len"),
        F.rank().over(w_title_len_by_release_date_ti).alias("release_date_rank"),
    )
)

leaders = metrics.filter(F.col("release_date_rank") <= 2)

with_trends = (
    leaders
    .select(
        "*",
        F.size(F.collect_set("artist_id").over(w_release_date_year_duration_s)).alias("distinct_artist_id_in_release_date_year_duration"),
        F.round(F.cume_dist().over(w_release_date_decile_by_days), 3).alias("days_since_prev_release_date_cume_dist"),
        F.max("next_title_len").over(w_release_date_decile_by_days_2).alias("max_next_title_len_so_far"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "title_len",
        "duration_sec_per_bpm_avg_1_prior",
        "title_len_share_of_is_explicit",
        "rn",
        "duration_sec_per_bpm_seq",
        "release_date_decile",
        "release_date_rank",
        "distinct_artist_id_in_release_date_year_duration",
        "max_next_title_len_so_far",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
    .orderBy(F.col("duration_sec_per_bpm_avg_1_prior").desc(), "id")
    .limit(5)
)
