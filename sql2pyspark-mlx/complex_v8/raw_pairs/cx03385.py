from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_is_explic = Window.partitionBy(F.date_trunc("month", "release_date"), "is_explicit_label")
w_is_explicit_label_artist_id = Window.partitionBy("is_explicit_label", "artist_id")
w_is_explicit_label_by_artist = Window.partitionBy("is_explicit_label").orderBy("artist_debut_year", "id")
w_all_by_artist_debut_year_ful = Window.orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_label_by_artist_2 = Window.partitionBy("is_explicit_label").orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy("release_date", "id")
w_artist_debut_year_decile_run = Window.partitionBy("artist_debut_year_decile", "running_count_tier").orderBy(F.col("is_explicit_label_artist_id_max_artist_debut_yea").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_debut_year_decile_run_2 = Window.partitionBy("artist_debut_year_decile", "running_count_tier").orderBy(F.col("is_explicit_label_artist_id_max_artist_debut_yea").desc(), "id")
w_prev3_artist_debut_year_tier = Window.partitionBy("prev3_artist_debut_year_tier")
w_all_by_artist_debut_year_upp = Window.orderBy("artist_debut_year_upper_trim_title_like_e", "artist_id", "prev3_artist_debut_year_tier")

filtered = (
    df_tracks
    .filter(F.year(F.col("release_date")) == 2024)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
    )
)

with_parent_artist = (
    filtered.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit_label",
        "s.title_clean",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_trends = (
    with_parent_artist
    .select(
        "*",
        F.sum("artist_debut_year").over(w_release_date_month_is_explic).alias("release_date_month_is_explicit_label_sum_artist_"),
        F.sum("artist_debut_year").over(w_is_explicit_label_artist_id).alias("is_explicit_label_artist_id_sum_artist_debut_yea"),
        F.lag("release_date").over(w_is_explicit_label_by_artist).alias("prev_release_date"),
        F.count(F.lit(1)).over(w_release_date_month_is_explic).alias("rows_in_release_date_month_is_explicit_label"),
        F.ntile(10).over(w_is_explicit_label_by_artist).alias("artist_debut_year_decile"),
        F.nth_value("release_date", 2).over(w_all_by_artist_debut_year_ful).alias("second_release_date"),
        F.count(F.lit(1)).over(w_is_explicit_label_by_artist_2).alias("running_count"),
        F.lag("artist_debut_year", 3, 0).over(w_artist_id_by_release_date).alias("prev3_artist_debut_year"),
        F.max("artist_debut_year").over(w_is_explicit_label_artist_id).alias("is_explicit_label_artist_id_max_artist_debut_yea"),
        F.count(F.lit(1)).over(w_is_explicit_label_artist_id).alias("rows_in_is_explicit_label_artist_id"),
        F.round(F.col("artist_debut_year") / F.nullif(F.sum("artist_debut_year").over(w_is_explicit_label_artist_id), F.lit(0)), 4).alias("artist_debut_year_share_of_is_explicit_label_art"),
        F.round(F.avg("artist_debut_year").over(w_is_explicit_label_artist_id), 2).alias("is_explicit_label_artist_id_avg_artist_debut_yea"),
    )
)

tagged = (
    with_trends
    .select(
        "*",
        F.when(F.col("running_count") >= 14, "HIGH").when(F.col("running_count") >= 5, "MEDIUM").otherwise("LOW").alias("running_count_tier"),
        F.when(F.col("prev3_artist_debut_year") >= 2006, "HIGH").when(F.col("prev3_artist_debut_year") >= 1994, "MEDIUM").otherwise("LOW").alias("prev3_artist_debut_year_tier"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        F.sum("prev3_artist_debut_year").over(w_artist_debut_year_decile_run).alias("running_prev3_artist_debut_year"),
        (F.col("artist_debut_year_decile") - F.lag("artist_debut_year_decile").over(w_artist_debut_year_decile_run_2)).alias("artist_debut_year_decile_change"),
        F.min("artist_debut_year").over(w_prev3_artist_debut_year_tier).alias("prev3_artist_debut_year_tier_min_artist_debut_ye"),
        F.round(F.col("running_count") - F.avg("running_count").over(w_prev3_artist_debut_year_tier), 2).alias("running_count_vs_prev3_artist_debut_year_tier_av"),
    )
)

rollup_base = (
    windowed
    .groupBy("artist_id", "prev3_artist_debut_year_tier")
    .agg(
        F.min("running_prev3_artist_debut_year").alias("min_running_prev3_artist_debut_year"),
        F.sum("running_count_vs_prev3_artist_debut_year_tier_av").alias("total_running_count_vs_prev3_artist_debut_year_t"),
        F.percentile_approx("artist_debut_year_share_of_is_explicit_label_art", 0.5).alias("median_artist_debut_year_share_of_is_explicit_la"),
        F.sum(F.when(F.upper(F.trim(F.col("title"))).like("E%"), F.col("artist_debut_year")).otherwise(0)).alias("artist_debut_year_upper_trim_title_like_e"),
        F.sum("artist_debut_year_decile").alias("total_artist_debut_year_decile"),
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("title"))).like("R%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_title_like_r"),
        F.max("artist_debut_year_share_of_is_explicit_label_art").alias("max_artist_debut_year_share_of_is_explicit_label"),
    )
    .filter(F.col("max_artist_debut_year_share_of_is_explicit_label") > 0.4)
    .drop("max_artist_debut_year_share_of_is_explicit_label")
)

with_windows = (
    rollup_base
    .withColumn("pct_upper_trim_title_like_r_share_of_prev3_artis", F.round(F.col("pct_upper_trim_title_like_r") / F.nullif(F.sum("pct_upper_trim_title_like_r").over(w_prev3_artist_debut_year_tier), F.lit(0)), 4))
    .withColumn("next_pct_upper_trim_title_like_r", F.lead("pct_upper_trim_title_like_r").over(w_all_by_artist_debut_year_upp))
)

result = (
    with_windows
    .filter(F.col("next_pct_upper_trim_title_like_r") >= 45.0)
    .select(
        "artist_id",
        "prev3_artist_debut_year_tier",
        "min_running_prev3_artist_debut_year",
        "total_running_count_vs_prev3_artist_debut_year_t",
        "median_artist_debut_year_share_of_is_explicit_la",
        "artist_debut_year_upper_trim_title_like_e",
        "pct_upper_trim_title_like_r",
        "pct_upper_trim_title_like_r_share_of_prev3_artis",
        "next_pct_upper_trim_title_like_r",
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("next_pct_upper_trim_title_like_r").desc(),
    )
)
