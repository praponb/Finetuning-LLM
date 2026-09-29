from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_group_source_by_is_sk = Window.partitionBy("source_group", "source").orderBy("is_skipped_flag", "id")
w_source_group_source_by_is_sk_2 = Window.partitionBy("source_group", "source").orderBy("is_skipped_flag")
w_source = Window.partitionBy("source")
w_track_id = Window.partitionBy("track_id")
w_all_by_is_skipped_flag = Window.orderBy(F.col("is_skipped_flag").desc(), "id")
w_all_by_is_skipped_flag_runni = Window.orderBy(F.col("is_skipped_flag").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_running = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_group_source_by_is_sk_3 = Window.partitionBy("source_group", "source").orderBy("is_skipped_flag", "id").rowsBetween(-2, -1)

valid = (
    df_plays
    .filter(F.col("ms_played") >= 56858)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.when(F.col("source").isin("search", "radio"), "group_a").when(F.col("source") == "playlist", "group_b").otherwise("other").alias("source_group"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.round(F.cume_dist().over(w_source_group_source_by_is_sk), 3).alias("is_skipped_flag_cume_dist"),
        F.rank().over(w_source_group_source_by_is_sk_2).alias("is_skipped_flag_rank"),
        F.count(F.lit(1)).over(w_source).alias("rows_in_source"),
        F.round(F.cume_dist().over(w_source_group_source_by_is_sk_2), 3).alias("is_skipped_flag_cume_dist_2"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.row_number().over(w_all_by_is_skipped_flag).alias("seq_no"),
        F.round(F.col("is_skipped_flag") - F.avg("is_skipped_flag").over(w_source), 2).alias("is_skipped_flag_vs_source_avg"),
        F.first("ms_played", ignorenulls=True).over(w_all_by_is_skipped_flag_runni).alias("first_known_ms_played"),
        F.lag("ms_played", 1, 0).over(w_source_group_source_by_is_sk).alias("prev_ms_played"),
        F.round(F.col("is_skipped_flag") - F.avg("is_skipped_flag").over(w_track_id), 2).alias("is_skipped_flag_vs_track_id_avg"),
        F.sum("ms_played").over(w_all_by_ms_played_running).alias("running_ms_played"),
        F.sum("ms_played").over(w_source_group_source_by_is_sk_3).alias("ms_played_sum_2_prior"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_source), F.lit(0)), 4).alias("ms_played_share_of_source"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_id), F.lit(0)), 4).alias("ms_played_share_of_track_id"),
    )
)

top_per_group = (
    windowed
    .filter(F.col("is_skipped_flag_rank") <= 2)
    .drop("is_skipped_flag_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped_flag",
        "source_group",
        "is_skipped_flag_cume_dist",
        "rows_in_source",
        "is_skipped_flag_cume_dist_2",
        "seq_no",
        "is_skipped_flag_vs_source_avg",
        "first_known_ms_played",
        "prev_ms_played",
        "is_skipped_flag_vs_track_id_avg",
        "running_ms_played",
        "ms_played_sum_2_prior",
        "ms_played_share_of_source",
        F.lower(F.regexp_replace(F.trim(F.col("source_group")), " ", "_")).alias("source_group_slug"),
        F.least(F.col("rows_in_track_id"), F.col("prev_ms_played")).alias("least_rows_in_track_id_prev_ms_played"),
    )
    .orderBy(
        F.col("device").asc_nulls_last(),
        F.col("listener_id").desc_nulls_first(),
    )
)
