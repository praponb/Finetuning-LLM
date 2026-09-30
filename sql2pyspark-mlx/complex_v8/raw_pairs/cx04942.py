from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tracks = (
    df_tracks
    .filter(F.col("bpm") >= 100)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

sequenced = (
    filtered_tracks
    .select(
        "*",
        F.round(F.avg("duration_sec").over(w_artist_id), 2).alias("artist_id_avg_duration_sec"),
        F.first("bpm", ignorenulls=True).over(w_artist_id_is_explicit_by_dur).alias("first_known_bpm"),
        F.count(F.lit(1)).over(w_all_by_duration_sec_running).alias("running_count"),
    )
)

stats = (
    sequenced
    .filter(~F.col("is_explicit"))
    .groupBy(F.date_format("release_date", "yyyy-MM").alias("release_date_ym"))
    .agg(
        F.sum("first_known_bpm").alias("total_first_known_bpm"),
        F.min("release_date").alias("first_release_date"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("running_count").alias("total_running_count"),
        F.avg("running_count").alias("avg_running_count_check"),
    )
    .filter(F.col("avg_running_count_check") > 5)
    .drop("avg_running_count_check")
)

derived = (
    stats
    .select(
        "release_date_ym",
        "first_release_date",
        "cnt",
        "distinct_ids",
        "total_running_count",
        F.lower(F.regexp_replace(F.trim(F.col("release_date_ym")), " ", "_")).alias("release_date_ym_slug"),
    )
)

result = (
    derived
    .rollup("cnt", "distinct_ids")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.avg("distinct_ids"), 2).alias("avg_distinct_ids"),
        F.max("cnt").alias("max_cnt"),
    )
    .orderBy(F.col("cnt").asc_nulls_last(), F.col("distinct_ids").asc_nulls_last())
)
