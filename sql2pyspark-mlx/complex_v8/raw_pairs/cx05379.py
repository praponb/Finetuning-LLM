from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_ms_played = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc(), "id")
w_played_at_month_device = Window.partitionBy(F.month("played_at"), "device")
w_all_by_ms_played_full = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_by_ms_played_prior = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-6, -1)
w_played_at_year = Window.partitionBy(F.year("played_at"))
w_is_skipped_source_by_played = Window.partitionBy("is_skipped", "source").orderBy(F.col("played_at").desc(), "id")
w_all_by_ms_played = Window.orderBy(F.col("ms_played").asc_nulls_last(), "id")
w_is_skipped_source_by_played_2 = Window.partitionBy("is_skipped", "source").orderBy(F.col("played_at").desc())
w_track_id_played_at_month_by = Window.partitionBy("track_id", F.date_trunc("month", "played_at")).orderBy("ms_played", "id")
w_track_id_played_at_month_by_2 = Window.partitionBy("track_id", F.date_trunc("month", "played_at")).orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ms_played_dense_rank = Window.partitionBy("ms_played_dense_rank")
w_source_by_ms_played_dense_ra = Window.partitionBy("source").orderBy(F.col("ms_played_dense_rank_2_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_by_ms_played_dense_ra_2 = Window.partitionBy("source").orderBy(F.col("ms_played_dense_rank_2_bucket").desc(), "id")

base_plays = (
    df_plays
    .filter((F.col("device").isin("speaker", "phone", "desktop")) | (F.col("ms_played") < 291723))
)

windowed = (
    base_plays
    .select(
        "id",
        "played_at",
        "is_skipped",
        "source",
        F.row_number().over(w_track_id_by_ms_played).alias("seq_no"),
        F.round(F.avg("ms_played").over(w_played_at_month_device), 2).alias("played_at_month_device_avg_ms_played"),
        F.first("ms_played").over(w_all_by_ms_played_full).alias("first_ms_played"),
        F.sum("ms_played").over(w_track_id_by_ms_played_prior).alias("ms_played_sum_6_prior"),
        F.lead("played_at").over(w_track_id_by_ms_played).alias("next_played_at"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_played_at_year), F.lit(0)), 4).alias("ms_played_share_of_played_at_year"),
        F.row_number().over(w_is_skipped_source_by_played).alias("played_at_seq"),
        F.dense_rank().over(w_all_by_ms_played).alias("ms_played_dense_rank"),
        F.round(F.cume_dist().over(w_is_skipped_source_by_played_2), 3).alias("played_at_cume_dist"),
        F.dense_rank().over(w_track_id_by_ms_played).alias("ms_played_dense_rank_2"),
        F.row_number().over(w_track_id_played_at_month_by).alias("rn"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_played_at_month_device), F.lit(0)), 4).alias("ms_played_share_of_played_at_month_device"),
        F.nth_value("ms_played", 2).over(w_track_id_played_at_month_by_2).alias("second_ms_played"),
    )
)

labeled = (
    windowed
    .select(
        "id",
        "played_at",
        "is_skipped",
        "source",
        "seq_no",
        "played_at_month_device_avg_ms_played",
        "first_ms_played",
        "ms_played_share_of_played_at_year",
        "played_at_seq",
        "ms_played_dense_rank",
        "played_at_cume_dist",
        "ms_played_dense_rank_2",
        "second_ms_played",
        (F.floor(F.col("ms_played_dense_rank_2") / 2) * 2).alias("ms_played_dense_rank_2_bucket"),
        F.round((F.unix_timestamp(F.col("played_at")) - F.unix_timestamp(F.col("next_played_at"))) / 3600, 1).alias("hours_next_played_at_to_played_at"),
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
    )
)

sequenced = (
    labeled
    .select(
        "*",
        F.count(F.lit(1)).over(w_ms_played_dense_rank).alias("rows_in_ms_played_dense_rank"),
        F.max("seq_no").over(w_ms_played_dense_rank).alias("ms_played_dense_rank_max_seq_no"),
        F.first("ms_played_dense_rank_2", ignorenulls=True).over(w_source_by_ms_played_dense_ra).alias("first_known_ms_played_dense_rank_2"),
        F.lag("second_ms_played").over(w_source_by_ms_played_dense_ra_2).alias("prev_second_ms_played"),
    )
)

result = (
    sequenced
    .filter(F.col("ms_played_dense_rank_max_seq_no") <= 60)
    .select(
        "id",
        "is_skipped",
        "source",
        "seq_no",
        "played_at_month_device_avg_ms_played",
        "ms_played_share_of_played_at_year",
        "played_at_seq",
        "ms_played_dense_rank",
        "played_at_cume_dist",
        "ms_played_dense_rank_2",
        "second_ms_played",
        "hours_next_played_at_to_played_at",
        "source_prefix",
        "rows_in_ms_played_dense_rank",
        "ms_played_dense_rank_max_seq_no",
        "first_known_ms_played_dense_rank_2",
        "prev_second_ms_played",
        F.lower(F.regexp_replace(F.trim(F.col("source_prefix")), " ", "_")).alias("source_prefix_slug"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
