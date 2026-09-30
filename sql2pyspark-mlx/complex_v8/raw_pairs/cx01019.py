from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_distinct_track_ids_by = Window.partitionBy("source", "distinct_track_ids").orderBy("row_count", "source", "is_skipped").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_distinct_track_ids_by_2 = Window.partitionBy("source", "distinct_track_ids").orderBy("row_count", "source", "is_skipped").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

totals = (
    base
    .groupBy("source", "is_skipped")
    .agg(
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("source") == "radio", 1)).alias("n_source_radio"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
        F.countDistinct("source").alias("distinct_sources"),
    )
)

windowed = (
    totals
    .select(
        "source",
        "is_skipped",
        "source_list",
        "row_count",
        "n_source_radio",
        F.first("distinct_sources").over(w_source_distinct_track_ids_by).alias("first_distinct_sources"),
        F.max("n_source_radio").over(w_source_distinct_track_ids_by_2).alias("max_n_source_radio_so_far"),
    )
)

result = (
    windowed
    .select(
        "source",
        "is_skipped",
        "source_list",
        "row_count",
        "n_source_radio",
        "first_distinct_sources",
        "max_n_source_radio_so_far",
    )
    .orderBy(
        F.col("max_n_source_radio_so_far").asc_nulls_last(),
        "source",
        "is_skipped",
    )
    .limit(20)
)
