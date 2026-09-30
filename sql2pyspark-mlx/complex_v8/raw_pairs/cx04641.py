from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_listener_id_by_ms = Window.partitionBy("is_skipped", "listener_id").orderBy(F.col("ms_played").desc(), "id")
w_is_skipped_listener_id_by_ms_2 = Window.partitionBy("is_skipped", "listener_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_device = Window.partitionBy("is_skipped", "device")
w_dedup = Window.partitionBy("device", "listener_id").orderBy(F.col("first_ms_played").desc(), F.col("id").desc())

scoped_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("track_id").cast("string")).alias("source_ref"),
    )
)

ranked = (
    scoped_plays
    .select(
        "*",
        F.ntile(5).over(w_is_skipped_listener_id_by_ms).alias("ms_played_quintile"),
        F.first("ms_played").over(w_is_skipped_listener_id_by_ms_2).alias("first_ms_played"),
        F.sum("ms_played").over(w_is_skipped_device).alias("is_skipped_device_sum_ms_played"),
        F.lead("ms_played").over(w_is_skipped_listener_id_by_ms).alias("next_ms_played"),
        F.rank().over(w_is_skipped_listener_id_by_ms).alias("ms_played_rank"),
    )
)

latest = (
    ranked
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .groupingSets(
        [["device", "source"], ["device"], []],
        "device", "source",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.sum("first_ms_played") / F.nullif(F.sum("next_ms_played"), F.lit(0)), 3).alias("first_ms_played_to_next_ms_played_ratio"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
    )
    .select(
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        F.coalesce(F.col("source"), F.lit("ALL")).alias("source"),
        "gid",
        "first_ms_played_to_next_ms_played_ratio",
        "source_list",
    )
    .orderBy(F.col("device").asc_nulls_last(), F.col("source").asc_nulls_last())
)
