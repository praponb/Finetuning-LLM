from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_by_releas = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy(F.col("release_date").desc_nulls_first())
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_release_date_month_by_releas_2 = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy(F.col("release_date").desc_nulls_first(), "id")
w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").desc(), "id")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_all_by_bpm_running = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2025-04-02").cast("date")) & (F.upper(F.trim(F.col("title"))).like("S%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "moods",
        F.round(F.percent_rank().over(w_release_date_month_by_releas), 3).alias("release_date_percent_rank"),
        F.lead("release_date").over(w_is_explicit_by_bpm).alias("next_release_date"),
        F.max("bpm").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_max_bpm"),
        F.lead("release_date").over(w_release_date_month_by_releas_2).alias("next_release_date_2"),
        F.dense_rank().over(w_is_explicit_artist_id_by_rel).alias("release_date_dense_rank"),
        F.count(F.lit(1)).over(w_artist_id_is_explicit).alias("rows_in_artist_id_is_explicit"),
        F.last("release_date", ignorenulls=True).over(w_all_by_bpm_running).alias("last_known_release_date"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("duration_sec_share_of_artist_id_is_explicit"),
        F.count(F.lit(1)).over(w_is_explicit_artist_id).alias("rows_in_is_explicit_artist_id"),
        F.round(F.cume_dist().over(w_release_date_month_by_releas), 3).alias("release_date_cume_dist"),
    )
)

annotated = (
    with_windows
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "release_date_percent_rank",
        "next_release_date",
        "is_explicit_artist_id_max_bpm",
        "next_release_date_2",
        "release_date_dense_rank",
        "rows_in_artist_id_is_explicit",
        "duration_sec_share_of_artist_id_is_explicit",
        "release_date_cume_dist",
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref"),
        F.quarter(F.col("next_release_date")).alias("next_release_date_quarter"),
    )
)

result = (
    annotated
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "release_date_percent_rank",
        "next_release_date",
        "is_explicit_artist_id_max_bpm",
        "next_release_date_2",
        "rows_in_artist_id_is_explicit",
        "release_date_cume_dist",
        "title_clean",
        "title_ref",
        "next_release_date_quarter",
    )
    .orderBy(
        F.col("is_explicit_artist_id_max_bpm").desc(),
        F.col("release_date_percent_rank").desc_nulls_first(),
    )
)
