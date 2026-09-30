from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id")
w_is_explicit_flag_artist_id_b = Window.partitionBy("is_explicit_flag", "artist_id").orderBy("is_explicit_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_is_explicit_flag = Window.partitionBy("artist_id", "is_explicit_flag")
w_all_by_days_since_release_da = Window.orderBy("days_since_release_date", "id").rowsBetween(-1, Window.currentRow)
w_all_by_days_since_release_da_2 = Window.orderBy("days_since_release_date")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_days_since_release_da_3 = Window.orderBy("days_since_release_date", "id").rowsBetween(-6, Window.currentRow)
w_all_by_days_since_release_da_4 = Window.orderBy(F.col("days_since_release_date").desc(), "id")
w_all_by_days_since_release_da_5 = Window.orderBy("days_since_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_next3_release_date_year_next = Window.partitionBy(F.year("next3_release_date"), F.month("next_release_date"))
w_prev3_is_explicit_flag_is_ex = Window.partitionBy("prev3_is_explicit_flag", "is_explicit_flag").orderBy("prev3_is_explicit_flag")
w_days_since_release_date_dens = Window.partitionBy("days_since_release_date_dense_rank").orderBy(F.col("next3_release_date").desc(), "id").rowsBetween(-6, -1)

recent = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
)

windowed = (
    recent
    .select(
        "*",
        F.lead("release_date").over(w_artist_id_by_release_date).alias("next_release_date"),
        F.count(F.lit(1)).over(w_is_explicit_flag_artist_id_b).alias("running_count"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id_is_explicit_flag), F.lit(0)), 4).alias("duration_sec_share_of_artist_id_is_explicit_flag"),
        F.round(F.cume_dist().over(w_artist_id_by_release_date), 3).alias("release_date_cume_dist"),
        F.round(F.avg("duration_sec").over(w_all_by_days_since_release_da), 2).alias("duration_sec_avg_2"),
        F.dense_rank().over(w_all_by_days_since_release_da_2).alias("days_since_release_date_dense_rank"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.round(F.avg("duration_sec").over(w_all_by_days_since_release_da_3), 2).alias("duration_sec_avg_7"),
        F.lead("release_date", 3).over(w_all_by_days_since_release_da_4).alias("next3_release_date"),
        F.lag("is_explicit_flag", 3, 0).over(w_all_by_days_since_release_da_4).alias("prev3_is_explicit_flag"),
        F.last("duration_sec", ignorenulls=True).over(w_all_by_days_since_release_da_5).alias("last_known_duration_sec"),
    )
)

metrics = (
    windowed
    .select(
        "id",
        "title",
        "release_date",
        "moods",
        "days_since_release_date",
        "is_explicit_flag",
        "next_release_date",
        "running_count",
        "duration_sec_share_of_artist_id_is_explicit_flag",
        "release_date_cume_dist",
        "days_since_release_date_dense_rank",
        "rows_in_artist_id",
        "prev3_is_explicit_flag",
        F.min("duration_sec_avg_2").over(w_next3_release_date_year_next).alias("next3_release_date_year_next_release_date_month_"),
        F.round(F.cume_dist().over(w_prev3_is_explicit_flag_is_ex), 3).alias("prev3_is_explicit_flag_cume_dist"),
        F.round(F.avg("duration_sec").over(w_days_since_release_date_dens), 2).alias("duration_sec_avg_6_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "title",
        "release_date",
        "moods",
        "days_since_release_date",
        "is_explicit_flag",
        "next_release_date",
        "running_count",
        "days_since_release_date_dense_rank",
        "rows_in_artist_id",
        "prev3_is_explicit_flag_cume_dist",
        "duration_sec_avg_6_prior",
    )
    .orderBy(F.col("id").asc(), F.col("title").desc())
)
