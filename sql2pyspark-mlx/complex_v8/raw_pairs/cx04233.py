from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_prior_1 = Window.orderBy("release_date", "id").rowsBetween(-1, -1)
w_all_by_release_date_running = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_month = Window.partitionBy(F.date_trunc("month", "release_date"))
w_artist_id_artist_country = Window.partitionBy("artist_id", "artist_country")
w_artist_country_by_duration_s = Window.partitionBy("artist_country").orderBy("duration_sec_vs_release_date_month_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_country_by_duration_s_2 = Window.partitionBy("artist_country").orderBy("duration_sec_vs_release_date_month_avg", "id")

base = (
    df_tracks
    .filter((F.col("title").isNotNull()) & (F.col("release_date") < F.lit("2025-08-07").cast("date")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
    )
)

screened = (
    base.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

windowed = (
    screened
    .select(
        "*",
        F.round(F.avg("duration_sec").over(w_all_by_release_date_prior_1), 2).alias("duration_sec_avg_1_prior"),
        F.first("duration_sec", ignorenulls=True).over(w_all_by_release_date_running).alias("first_known_duration_sec"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_release_date_month), 2).alias("duration_sec_vs_release_date_month_avg"),
    )
)

enriched_artist = (
    windowed.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.is_verified")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.first_known_duration_sec",
        "s.duration_sec_vs_release_date_month_avg",
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
    )
)

with_windows = (
    enriched_artist
    .select(
        "*",
        F.count(F.lit(1)).over(w_artist_id_artist_country).alias("rows_in_artist_id_artist_country"),
        F.round(F.col("first_known_duration_sec") - F.avg("first_known_duration_sec").over(w_artist_id_artist_country), 2).alias("first_known_duration_sec_vs_artist_id_artist_cou"),
        F.nth_value("first_known_duration_sec", 2).over(w_artist_country_by_duration_s).alias("second_first_known_duration_sec"),
        F.round(F.col("first_known_duration_sec") / F.nullif(F.sum("first_known_duration_sec").over(w_artist_id_artist_country), F.lit(0)), 4).alias("first_known_duration_sec_share_of_artist_id_arti"),
        F.row_number().over(w_artist_country_by_duration_s_2).alias("seq_no"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_id",
        "first_known_duration_sec",
        "duration_sec_vs_release_date_month_avg",
        "artist_country",
        "rows_in_artist_id_artist_country",
        "first_known_duration_sec_vs_artist_id_artist_cou",
        "second_first_known_duration_sec",
        "seq_no",
        (F.floor(F.col("first_known_duration_sec") / 50) * 50).alias("first_known_duration_sec_bucket"),
    )
    .orderBy(
        F.col("duration_sec_vs_release_date_month_avg").desc_nulls_first(),
        F.col("first_known_duration_sec_vs_artist_id_artist_cou").desc(),
    )
)
