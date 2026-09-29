from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_release_date_mo = Window.partitionBy("artist_id").orderBy("release_date_month_start")
w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm_per_duration_sec", "id").rowsBetween(-6, -1)
w_release_date_year = Window.partitionBy(F.year("release_date"))
w_artist_id_by_release_date_mo_2 = Window.partitionBy("artist_id").orderBy("release_date_month_start", "id")
w_artist_id_by_release_date_mo_3 = Window.partitionBy("artist_id").orderBy("release_date_month_start", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_tracks
    .filter((F.length(F.trim(F.col("title"))) >= 4) & (F.col("bpm") >= 96))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.trunc(F.col("release_date"), "MM").alias("release_date_month_start"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

sequenced = (
    scoped
    .select(
        "*",
        F.rank().over(w_artist_id_by_release_date_mo).alias("release_date_month_start_rank"),
        F.sum("bpm_per_duration_sec").over(w_artist_id_is_explicit_by_bpm).alias("bpm_per_duration_sec_sum_6_prior"),
        F.size(F.collect_set("artist_id").over(w_release_date_year)).alias("distinct_artist_id_in_release_date_year"),
        F.lag("bpm", 2).over(w_artist_id_by_release_date_mo_2).alias("prev2_bpm"),
        F.count(F.lit(1)).over(w_release_date_year).alias("rows_in_release_date_year"),
        F.nth_value("duration_sec", 2).over(w_artist_id_by_release_date_mo_3).alias("second_duration_sec"),
    )
)

joined_artist = (
    sequenced.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select("s.*", F.col("a.country").alias("artist_country"))
)

screened = (
    joined_artist
    .crossJoin(joined_artist.agg(F.avg("bpm_per_duration_sec_sum_6_prior").alias("overall_avg_bpm_per_duration_sec_sum_6_prior")))
    .filter(F.col("bpm_per_duration_sec_sum_6_prior") > F.col("overall_avg_bpm_per_duration_sec_sum_6_prior"))
    .drop("overall_avg_bpm_per_duration_sec_sum_6_prior")
)

result = (
    screened
    .select("id", "duration_sec", "distinct_artist_id_in_release_date_year")
    .unpivot(
        ids=["id"],
        values=["duration_sec", "distinct_artist_id_in_release_date_year"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric_name")
)
