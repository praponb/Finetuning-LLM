from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_listener_id_by_ms = Window.partitionBy("is_skipped", "listener_id").orderBy(F.col("ms_played_bucket").desc_nulls_first(), "id")
w_is_skipped_listener_id_by_ms_2 = Window.partitionBy("is_skipped", "listener_id").orderBy(F.col("ms_played_bucket").desc_nulls_first())

scoped_plays = (
    df_plays
    .filter((~F.col("source").isin("search", "playlist")) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

with_trends = (
    scoped_plays
    .select(
        "id",
        "listener_id",
        "played_at",
        "is_skipped",
        "ms_played_bucket",
        (F.col("ms_played_bucket") - F.lag("ms_played_bucket").over(w_is_skipped_listener_id_by_ms)).alias("ms_played_bucket_change"),
        F.dense_rank().over(w_is_skipped_listener_id_by_ms_2).alias("ms_played_bucket_dense_rank"),
        F.rank().over(w_is_skipped_listener_id_by_ms).alias("ms_played_bucket_rank"),
    )
)

with_details_listener = (
    with_trends.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.is_skipped",
        "s.ms_played_bucket_change",
        "s.ms_played_bucket_rank",
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

stats = (
    with_details_listener
    .groupBy("is_skipped")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("ms_played_bucket_rank") / F.nullif(F.sum("ms_played_bucket_change"), F.lit(0)), 3).alias("ms_played_bucket_rank_to_ms_played_bucket_change"),
        F.max("ms_played_bucket_rank").alias("max_ms_played_bucket_rank_check"),
    )
    .filter(F.col("max_ms_played_bucket_rank_check") > 2)
    .drop("max_ms_played_bucket_rank_check")
)

result = (
    stats
    .select(
        "is_skipped",
        "distinct_ids",
        F.abs(F.col("distinct_ids") - 4).alias("distinct_ids_distance"),
    )
    .orderBy("distinct_ids_distance", "is_skipped")
    .limit(20)
)
