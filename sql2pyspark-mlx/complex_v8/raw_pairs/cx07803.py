from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped = Window.partitionBy("is_skipped")
w_device_by_played_at_dayofwee = Window.partitionBy("device").orderBy(F.col("played_at_dayofweek").desc(), "id").rowsBetween(-3, Window.currentRow)
w_track_id_played_at_to_date_b = Window.partitionBy("track_id", F.to_date("played_at")).orderBy(F.col("played_at_dayofweek").desc_nulls_first(), "id")
w_track_id_played_at_to_date_b_2 = Window.partitionBy("track_id", F.to_date("played_at")).orderBy(F.col("played_at_dayofweek").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_by_played_at_dayofw = Window.partitionBy("track_id").orderBy("played_at_dayofweek", "id")
w_track_id_by_played_at_dayofw_2 = Window.partitionBy("track_id").orderBy("played_at_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_played_at_to_date_b_3 = Window.partitionBy("track_id", F.to_date("played_at")).orderBy(F.col("played_at_dayofweek").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_n_rows_by_distinct_ids = Window.partitionBy("n_rows").orderBy(F.col("distinct_ids").desc(), "greatest_played_at_dayofweek_rn")

filtered_plays = (
    df_plays
    .filter((F.col("device").isNotNull()) & (F.col("source") == "radio"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
        F.dayofweek(F.col("played_at")).alias("played_at_dayofweek"),
    )
)

sequenced = (
    filtered_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "played_at_label",
        "played_at_dayofweek",
        F.max("ms_played").over(w_is_skipped).alias("is_skipped_max_ms_played"),
        F.round(F.avg("ms_played").over(w_device_by_played_at_dayofwee), 2).alias("ms_played_avg_4"),
        F.lag("played_at", 3).over(w_track_id_played_at_to_date_b).alias("prev3_played_at"),
        F.last("played_at_dayofweek", ignorenulls=True).over(w_track_id_played_at_to_date_b_2).alias("last_known_played_at_dayofweek"),
        F.row_number().over(w_track_id_by_played_at_dayofw).alias("rn"),
        F.last("ms_played").over(w_track_id_by_played_at_dayofw_2).alias("last_ms_played"),
        F.last("played_at").over(w_track_id_by_played_at_dayofw_2).alias("last_played_at"),
        F.round(F.avg("played_at_dayofweek").over(w_track_id_played_at_to_date_b_3), 2).alias("played_at_dayofweek_avg_6_prior"),
    )
)

derived = (
    sequenced
    .select(
        "*",
        F.greatest(F.col("played_at_dayofweek"), F.col("rn")).alias("greatest_played_at_dayofweek_rn"),
        F.abs(F.col("played_at_dayofweek") - 3).alias("played_at_dayofweek_distance"),
    )
)

grouped = (
    derived
    .groupBy("greatest_played_at_dayofweek_rn")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
        F.max("last_played_at").alias("last_last_played_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
    )
)

windowed = (
    grouped
    .select(
        "*",
        F.lead("distinct_listener_ids", 3).over(w_n_rows_by_distinct_ids).alias("next3_distinct_listener_ids"),
        F.ntile(3).over(w_n_rows_by_distinct_ids).alias("distinct_ids_tercile"),
    )
)

result = (
    windowed
    .filter(F.col("last_last_played_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp"))
    .select(
        "greatest_played_at_dayofweek_rn",
        "n_rows",
        "distinct_track_ids",
        "last_last_played_at",
        "distinct_ids",
        "distinct_listener_ids",
        "next3_distinct_listener_ids",
        "distinct_ids_tercile",
        F.round(F.col("distinct_ids_tercile") / F.nullif(F.col("n_rows"), F.lit(0)), 3).alias("distinct_ids_tercile_per_n_rows"),
    )
    .orderBy(F.col("greatest_played_at_dayofweek_rn").desc_nulls_first(), "n_rows")
)
