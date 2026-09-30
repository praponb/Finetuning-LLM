from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_by_artist = Window.partitionBy(F.month("release_date")).orderBy(F.col("artist_debut_year").desc(), "id")
w_artist_id_by_release_date_mo = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_release_date_month_by_artist_2 = Window.partitionBy(F.month("release_date")).orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id")
w_artist_id_by_release_date_fu = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_artist_id_avg_a = Window.partitionBy("artist_id").orderBy(F.col("artist_id_avg_artist_debut_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month = Window.partitionBy(F.date_trunc("month", "release_date"))
w_all_by_next_artist_debut_yea = Window.orderBy(F.col("next_artist_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.date_trunc("month", "release_date"))
w_distinct_ids_release_date_mo = Window.partitionBy("distinct_ids", F.date_trunc("month", "release_date_month")).orderBy("first_release_date")
w_release_date_month_month = Window.partitionBy(F.month("release_date_month"))
w_distinct_ids_release_date_mo_2 = Window.partitionBy("distinct_ids", F.date_trunc("month", "release_date_month")).orderBy("first_release_date", "release_date_month")
w_distinct_ids_first_release_d = Window.partitionBy("distinct_ids", F.month("first_release_date")).orderBy("total_last_artist_debut_year", "release_date_month").rowsBetween(-1, Window.currentRow)
w_distinct_ids_release_date_mo_3 = Window.partitionBy("distinct_ids", F.date_trunc("month", "release_date_month")).orderBy("first_release_date", "release_date_month").rowsBetween(-2, -1)

filtered = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.array_contains(F.col("moods"), "workout")))
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
    )
)

combined_details_artist = (
    filtered.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.is_verified")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.moods",
        "s.title_ref",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

windowed = (
    combined_details_artist
    .select(
        "*",
        F.lag("artist_debut_year").over(w_release_date_month_by_artist).alias("prev_artist_debut_year"),
        F.max("artist_debut_year").over(w_artist_id_by_release_date_mo).alias("artist_debut_year_max_4"),
        F.last("artist_debut_year", ignorenulls=True).over(w_release_date_month_by_artist_2).alias("last_known_artist_debut_year"),
        F.min("artist_debut_year").over(w_artist_id).alias("artist_id_min_artist_debut_year"),
        F.round(F.percent_rank().over(w_artist_id_by_release_date), 3).alias("release_date_percent_rank"),
        F.max("artist_debut_year").over(w_artist_id).alias("artist_id_max_artist_debut_year"),
        F.round(F.avg("artist_debut_year").over(w_artist_id), 2).alias("artist_id_avg_artist_debut_year"),
        F.lead("artist_debut_year", 1, 0).over(w_release_date_month_by_artist).alias("next_artist_debut_year"),
        F.last("artist_debut_year").over(w_artist_id_by_release_date_fu).alias("last_artist_debut_year"),
    )
)

metrics = (
    windowed
    .select(
        "id",
        "release_date",
        "moods",
        "artist_debut_year",
        "artist_debut_year_max_4",
        "artist_id_min_artist_debut_year",
        "release_date_percent_rank",
        "artist_id_max_artist_debut_year",
        "next_artist_debut_year",
        "last_artist_debut_year",
        F.nth_value("prev_artist_debut_year", 2).over(w_artist_id_by_artist_id_avg_a).alias("second_prev_artist_debut_year"),
        F.count(F.lit(1)).over(w_release_date_month).alias("rows_in_release_date_month"),
        F.round(F.col("prev_artist_debut_year") / F.nullif(F.sum("prev_artist_debut_year").over(w_release_date_month), F.lit(0)), 4).alias("prev_artist_debut_year_share_of_release_date_mon"),
        F.first("artist_debut_year").over(w_all_by_next_artist_debut_yea).alias("first_artist_debut_year"),
        F.min("prev_artist_debut_year").over(w_artist_id_release_date_month).alias("artist_id_release_date_month_min_prev_artist_deb"),
    )
)

rollup_base = (
    metrics
    .filter(F.col("artist_id_max_artist_debut_year") < 18406)
    .groupBy(F.date_trunc("month", "release_date").alias("release_date_month"))
    .agg(
        F.min("release_date").alias("first_release_date"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("artist_id_release_date_month_min_prev_artist_deb", 0.5).alias("median_artist_id_release_date_month_min_prev_art"),
        F.sum("last_artist_debut_year").alias("total_last_artist_debut_year"),
    )
)

metrics_2 = (
    rollup_base
    .select(
        "*",
        F.round(F.percent_rank().over(w_distinct_ids_release_date_mo), 3).alias("first_release_date_percent_rank"),
        F.row_number().over(w_distinct_ids_release_date_mo_2).alias("first_release_date_seq"),
        F.round(F.avg("distinct_ids").over(w_distinct_ids_first_release_d), 2).alias("distinct_ids_avg_2"),
        F.max("total_last_artist_debut_year").over(w_distinct_ids_release_date_mo_3).alias("total_last_artist_debut_year_max_2_prior"),
    )
)

result = (
    metrics_2
    .select(
        "release_date_month",
        "first_release_date",
        "distinct_ids",
        "median_artist_id_release_date_month_min_prev_art",
        "total_last_artist_debut_year",
        "first_release_date_percent_rank",
        "distinct_ids_avg_2",
        "total_last_artist_debut_year_max_2_prior",
        F.when(F.col("first_release_date_seq") >= 8, "A").when(F.col("first_release_date_seq") >= 3, "B").otherwise("C").alias("first_release_date_seq_tier"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("first_release_date")).alias("days_since_first_release_date"),
    )
    .orderBy(
        F.col("days_since_first_release_date").desc(),
        F.col("total_last_artist_debut_year_max_2_prior").desc(),
    )
)
