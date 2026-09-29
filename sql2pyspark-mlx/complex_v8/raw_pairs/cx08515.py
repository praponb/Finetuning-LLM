from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_release_date_quarter_is_expl = Window.partitionBy("release_date_quarter", "is_explicit").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_all_by_duration_sec_ties = Window.orderBy(F.col("duration_sec").desc())
w_all_by_duration_sec_full = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_quarter_is_expl_2 = Window.partitionBy("release_date_quarter", "is_explicit").orderBy(F.col("duration_sec").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_tracks
    .filter((~F.col("is_explicit")) | (F.size(F.col("moods")) >= 2))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.concat(F.col("title"), F.lit("#"), F.col("duration_sec").cast("string")).alias("title_ref"),
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.lead("release_date_quarter").over(w_release_date_quarter_is_expl).alias("next_release_date_quarter"),
        F.lag("bpm").over(w_all_by_duration_sec).alias("prev_bpm"),
        F.rank().over(w_all_by_duration_sec_ties).alias("duration_sec_rank"),
        F.last("release_date_quarter").over(w_all_by_duration_sec_full).alias("last_release_date_quarter"),
        F.round(F.col("release_date_quarter") - F.avg("release_date_quarter").over(w_artist_id), 2).alias("release_date_quarter_vs_artist_id_avg"),
        F.first("release_date_quarter", ignorenulls=True).over(w_release_date_quarter_is_expl_2).alias("first_known_release_date_quarter"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.nth_value("bpm", 2).over(w_all_by_duration_sec_full).alias("second_bpm"),
        F.lead("release_date").over(w_release_date_quarter_is_expl).alias("next_release_date"),
        F.nth_value("release_date", 2).over(w_all_by_duration_sec_full).alias("second_release_date"),
        F.nth_value("release_date_quarter", 2).over(w_all_by_duration_sec_full).alias("second_release_date_quarter"),
        F.round(F.cume_dist().over(w_release_date_quarter_is_expl), 3).alias("duration_sec_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "title_ref",
        "next_release_date_quarter",
        "prev_bpm",
        "duration_sec_rank",
        "last_release_date_quarter",
        "first_known_release_date_quarter",
        "rows_in_artist_id",
        "second_bpm",
        "next_release_date",
        "second_release_date",
        "second_release_date_quarter",
        "duration_sec_cume_dist",
        F.concat(F.col("title_ref"), F.lit("#"), F.col("prev_bpm").cast("string")).alias("title_ref_ref"),
    )
    .orderBy(F.col("artist_id").desc(), F.col("title_ref").desc())
)
