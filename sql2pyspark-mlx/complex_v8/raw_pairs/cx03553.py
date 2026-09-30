from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_release_date_month = Window.partitionBy("artist_id", "release_date_month").orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_release_date_month_2 = Window.partitionBy("artist_id", F.date_trunc("month", "release_date"))

recent = (
    df_tracks
    .filter((F.col("duration_sec") < 352) | (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        F.month(F.col("release_date")).alias("release_date_month"),
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
        F.abs(F.col("duration_sec") - 352).alias("duration_sec_distance"),
    )
)

sequenced = (
    recent
    .withColumn("first_bpm", F.first("bpm").over(w_artist_id_release_date_month))
    .withColumn("rows_in_artist_id_release_date_month", F.count(F.lit(1)).over(w_artist_id_release_date_month_2))
)

enriched = (
    sequenced
    .filter(F.col("rows_in_artist_id_release_date_month") < 17)
    .withColumn("duration_sec_distance_tier", F.when(F.col("duration_sec_distance") >= 252, "A").when(F.col("duration_sec_distance") >= 189, "B").otherwise("C"))
)

artist_id_stats = (
    enriched
    .groupBy("artist_id")
    .agg(F.avg("first_bpm").alias("avg_first_bpm"))
)

above_average = (
    enriched.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.first_bpm") > F.col("g.avg_first_bpm"))
    .select(
        "s.*",
        "g.avg_first_bpm",
        F.round(F.col("s.first_bpm") - F.col("g.avg_first_bpm"), 2).alias("first_bpm_minus_avg"),
    )
)

result = (
    above_average
    .select(
        "id",
        F.col("rows_in_artist_id_release_date_month").alias("rows_in_artist_id_release_date_month_v"),
        F.col("first_bpm").alias("first_bpm_v"),
        F.col("duration_sec_distance").alias("duration_sec_distance_v"),
        F.col("bpm").alias("bpm_v"),
    )
    .unpivot(
        ids=["id"],
        values=["rows_in_artist_id_release_date_month_v", "first_bpm_v", "duration_sec_distance_v", "bpm_v"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
