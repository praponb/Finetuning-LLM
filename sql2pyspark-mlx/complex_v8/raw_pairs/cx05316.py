from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_track_ids_by_distin = Window.partitionBy("distinct_track_ids").orderBy(F.col("distinct_track_ids_plus_n_is_skipped_true").desc(), "played_at_year")
w_row_count_by_distinct_track = Window.partitionBy("row_count").orderBy(F.col("distinct_track_ids").asc_nulls_last(), "played_at_year").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_row_count = Window.partitionBy("row_count")
w_distinct_track_ids_by_distin_2 = Window.partitionBy("distinct_track_ids").orderBy(F.col("distinct_track_ids_plus_n_is_skipped_true").desc(), "played_at_year").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-02-12 09:00:00").cast("timestamp")) & (F.col("source").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.quarter(F.col("played_at")).alias("played_at_quarter"),
        F.when(F.col("device").isin("speaker", "tv"), "group_a").when(F.col("device") == "desktop", "group_b").otherwise("other").alias("device_group"),
    )
)

rollup_base = (
    prepared
    .filter(F.col("ms_played") > 160264)
    .groupBy(F.year("played_at").alias("played_at_year"))
    .agg(
        F.count(F.when(F.col("is_skipped"), 1)).alias("n_is_skipped_true"),
        F.count("*").alias("row_count"),
        F.sum("played_at_quarter").alias("total_played_at_quarter"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
        F.sum(F.when(F.col("source") == "album", F.col("days_since_played_at")).otherwise(0)).alias("days_since_played_at_source_album"),
        F.sum("ms_played").alias("sum_ms_played_check"),
    )
    .filter(F.col("sum_ms_played_check") > 151112)
    .drop("sum_ms_played_check")
)

derived = (
    rollup_base
    .withColumn("distinct_track_ids_plus_n_is_skipped_true", F.col("distinct_track_ids") + F.col("n_is_skipped_true"))
)

ranked = (
    derived
    .select(
        "*",
        F.round(F.cume_dist().over(w_distinct_track_ids_by_distin), 3).alias("distinct_track_ids_plus_n_is_skipped_true_cume_d"),
        F.nth_value("total_played_at_quarter", 2).over(w_row_count_by_distinct_track).alias("second_total_played_at_quarter"),
        F.dense_rank().over(w_distinct_track_ids_by_distin).alias("distinct_track_ids_plus_n_is_skipped_true_dense_"),
        F.round(F.col("distinct_track_ids") - F.avg("distinct_track_ids").over(w_row_count), 2).alias("distinct_track_ids_vs_row_count_avg"),
        F.nth_value("n_is_skipped_true", 2).over(w_distinct_track_ids_by_distin_2).alias("second_n_is_skipped_true"),
    )
)

result = (
    ranked
    .select(
        "played_at_year",
        "n_is_skipped_true",
        "row_count",
        "source_list",
        "distinct_track_ids",
        "days_since_played_at_source_album",
        "distinct_track_ids_plus_n_is_skipped_true_cume_d",
        "second_total_played_at_quarter",
        "distinct_track_ids_plus_n_is_skipped_true_dense_",
        "second_n_is_skipped_true",
    )
    .orderBy("distinct_track_ids", F.col("played_at_year").desc())
)
