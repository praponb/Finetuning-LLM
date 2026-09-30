from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_label = Window.partitionBy("is_skipped_label")
w_all_by_ms_played = Window.orderBy(F.col("ms_played").desc(), "id")
w_listener_id_by_played_at_tie = Window.partitionBy("listener_id").orderBy(F.col("played_at").desc_nulls_first())
w_all_by_played_at_full = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_played_at = Window.partitionBy("listener_id").orderBy(F.col("played_at").desc_nulls_first(), "id")

valid_plays = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("ms_played") < 220498))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label_2"),
    )
)

sequenced = (
    valid_plays
    .select(
        "*",
        F.size(F.collect_set("listener_id").over(w_is_skipped_label)).alias("distinct_listener_id_in_is_skipped_label"),
        (F.col("ms_played") - F.lag("ms_played").over(w_all_by_ms_played)).alias("ms_played_change"),
        F.count(F.lit(1)).over(w_is_skipped_label).alias("rows_in_is_skipped_label"),
        F.sum("ms_played").over(w_is_skipped_label).alias("is_skipped_label_sum_ms_played"),
        F.dense_rank().over(w_listener_id_by_played_at_tie).alias("played_at_dense_rank"),
        F.last("ms_played").over(w_all_by_played_at_full).alias("last_ms_played"),
        F.rank().over(w_listener_id_by_played_at_tie).alias("played_at_rank"),
        F.min("ms_played").over(w_is_skipped_label).alias("is_skipped_label_min_ms_played"),
        F.round(F.avg("ms_played").over(w_is_skipped_label), 2).alias("is_skipped_label_avg_ms_played"),
        F.lag("played_at").over(w_listener_id_by_played_at).alias("prev_played_at"),
    )
)

result = (
    sequenced
    .filter(F.col("is_skipped_label_min_ms_played") < 2202250)
    .select(
        "id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped_label",
        "is_skipped_label_2",
        "ms_played_change",
        "rows_in_is_skipped_label",
        "is_skipped_label_sum_ms_played",
        "played_at_dense_rank",
        "last_ms_played",
        "played_at_rank",
        "is_skipped_label_avg_ms_played",
        "prev_played_at",
        F.round((F.unix_timestamp(F.col("played_at")) - F.unix_timestamp(F.col("prev_played_at"))) / 3600, 1).alias("hours_prev_played_at_to_played_at"),
        F.concat(F.col("is_skipped_label"), F.lit("#"), F.col("ms_played").cast("string")).alias("is_skipped_label_ref"),
    )
    .orderBy("rows_in_is_skipped_label", "is_skipped_label_2", "id")
    .limit(5)
)
