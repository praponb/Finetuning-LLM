from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec_pr = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-2, -1)
w_artist_country_by_bpm = Window.partitionBy("artist_country").orderBy("bpm", "id")
w_artist_country_by_bpm_prior = Window.partitionBy("artist_country").orderBy("bpm", "id").rowsBetween(-3, -1)
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_by_duration_sec_ru = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_artist_debut_ye = Window.partitionBy("artist_id").orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_artist_debut_ye_2 = Window.partitionBy("artist_id").orderBy(F.col("artist_debut_year").desc(), "id")
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_country_artist_is_ver = Window.partitionBy("artist_country", "artist_is_verified").orderBy(F.col("bpm").desc())
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_artist_debut_year_quintile_s = Window.partitionBy("artist_debut_year_quintile", "seq_no")

scoped_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("N%")) & (F.array_contains(F.col("moods"), "focus")))
)

joined = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

metrics = (
    joined
    .select(
        "*",
        F.round(F.avg("artist_debut_year").over(w_artist_id_by_duration_sec_pr), 2).alias("artist_debut_year_avg_2_prior"),
        (F.col("artist_debut_year") - F.lag("artist_debut_year").over(w_artist_country_by_bpm)).alias("artist_debut_year_change"),
        F.round(F.avg("bpm").over(w_artist_country_by_bpm_prior), 2).alias("bpm_avg_3_prior"),
        F.round(F.col("artist_debut_year") - F.avg("artist_debut_year").over(w_artist_id), 2).alias("artist_debut_year_vs_artist_id_avg"),
        F.max("bpm").over(w_artist_id).alias("artist_id_max_bpm"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_artist_id_by_duration_sec)).alias("days_since_prev_release_date"),
        F.round(F.avg("artist_debut_year").over(w_artist_id), 2).alias("artist_id_avg_artist_debut_year"),
        F.count(F.lit(1)).over(w_artist_id_by_duration_sec_ru).alias("running_count"),
        F.last("release_date", ignorenulls=True).over(w_artist_id_by_duration_sec_ru).alias("last_known_release_date"),
        F.last("duration_sec").over(w_artist_id_by_artist_debut_ye).alias("last_duration_sec"),
        F.row_number().over(w_artist_id_by_duration_sec).alias("seq_no"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_country_by_bpm)).alias("duration_sec_change"),
        F.sum("bpm").over(w_artist_id_by_duration_sec_ru).alias("running_bpm"),
        F.ntile(5).over(w_artist_id_by_artist_debut_ye_2).alias("artist_debut_year_quintile"),
        F.round(F.col("artist_debut_year") / F.nullif(F.sum("artist_debut_year").over(w_artist_id), F.lit(0)), 4).alias("artist_debut_year_share_of_artist_id"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.last("last_known_release_date", ignorenulls=True).over(w_all_by_duration_sec_running).alias("last_known_last_known_release_date"),
        F.rank().over(w_artist_country_artist_is_ver).alias("bpm_rank"),
        F.dense_rank().over(w_all_by_duration_sec_ties).alias("duration_sec_dense_rank"),
        F.max("duration_sec").over(w_artist_debut_year_quintile_s).alias("artist_debut_year_quintile_seq_no_max_duration_s"),
    )
)

rollup_base = (
    with_windows
    .groupBy(F.date_format("release_date", "yyyy-MM").alias("release_date_ym"))
    .agg(
        F.sum("artist_id_max_bpm").alias("total_artist_id_max_bpm"),
        F.round(F.sum("artist_debut_year_avg_2_prior") / F.nullif(F.sum("running_bpm"), F.lit(0)), 3).alias("artist_debut_year_avg_2_prior_to_running_bpm_rat"),
        F.max("days_since_prev_release_date").alias("max_days_since_prev_release_date"),
        F.avg("bpm_rank").alias("avg_bpm_rank_check"),
    )
    .filter(F.col("avg_bpm_rank_check") > 3)
    .drop("avg_bpm_rank_check")
)

result = (
    rollup_base
    .select(
        "release_date_ym",
        "total_artist_id_max_bpm",
        "artist_debut_year_avg_2_prior_to_running_bpm_rat",
        F.coalesce(F.col("total_artist_id_max_bpm"), F.lit(0)).alias("total_artist_id_max_bpm_filled"),
    )
)
