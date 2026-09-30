from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_ties = Window.orderBy("played_at")
w_device_is_skipped_label_by_p = Window.partitionBy("device", "is_skipped_label").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_device_is_skipped_label_by_p_2 = Window.partitionBy("device", "is_skipped_label").orderBy(F.col("played_at").desc(), "id")
w_all_by_played_at_2 = Window.orderBy("played_at", "id")
w_device_by_played_at_ties = Window.partitionBy("device").orderBy("played_at")
w_all_by_played_at_full = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

ranked = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w_all_by_played_at_ties).alias("played_at_dense_rank"),
        F.nth_value("played_at", 2).over(w_device_is_skipped_label_by_p).alias("second_played_at"),
        F.lag("played_at").over(w_all_by_played_at).alias("prev_played_at"),
        F.ntile(4).over(w_device_is_skipped_label_by_p_2).alias("played_at_quartile"),
        F.round(F.cume_dist().over(w_all_by_played_at_ties), 3).alias("played_at_cume_dist"),
        F.lag("played_at").over(w_device_is_skipped_label_by_p_2).alias("prev_played_at_2"),
        F.lead("played_at", 2).over(w_all_by_played_at_2).alias("next2_played_at"),
        F.round(F.percent_rank().over(w_device_by_played_at_ties), 3).alias("played_at_percent_rank"),
        F.nth_value("played_at", 2).over(w_all_by_played_at_full).alias("second_played_at_2"),
        F.row_number().over(w_all_by_played_at_2).alias("rn"),
    )
)

leaders = ranked.filter(F.col("played_at_dense_rank") == 1)

result = (
    leaders
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "is_skipped_label",
        "played_at_dense_rank",
        "second_played_at",
        "prev_played_at",
        "played_at_quartile",
        "played_at_cume_dist",
        "prev_played_at_2",
        "played_at_percent_rank",
        "second_played_at_2",
        "rn",
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
    )
)
