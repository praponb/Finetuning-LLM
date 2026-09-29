from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_listener_id_by_ms_p = Window.partitionBy("track_id", "listener_id").orderBy(F.col("ms_played").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped = Window.partitionBy("is_skipped")
w_track_id_listener_id_by_ms_p_2 = Window.partitionBy("track_id", "listener_id").orderBy(F.col("ms_played").asc_nulls_last())
w_track_id_listener_id_by_ms_p_3 = Window.partitionBy("track_id", "listener_id").orderBy(F.col("ms_played").asc_nulls_last(), "id")

filtered = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 225733, "HIGH").when(F.col("ms_played") >= 107912, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
        F.when(F.col("ms_played") >= 308104, "large").when(F.col("ms_played") >= 151112, "medium").otherwise("small").alias("ms_played_tier_2"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

annotated = (
    filtered
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

metrics = (
    annotated
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        F.last("ms_played").over(w_track_id_listener_id_by_ms_p).alias("last_ms_played"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_is_skipped), F.lit(0)), 4).alias("ms_played_share_of_is_skipped"),
        F.round(F.cume_dist().over(w_track_id_listener_id_by_ms_p_2), 3).alias("ms_played_cume_dist"),
        F.dense_rank().over(w_track_id_listener_id_by_ms_p_3).alias("ms_played_dense_rank"),
    )
)

result = (
    metrics
    .groupingSets(
        [["is_skipped", "listener_id", "source"], ["is_skipped"]],
        "is_skipped", "listener_id", "source",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("ms_played_share_of_is_skipped").alias("total_ms_played_share_of_is_skipped"),
    )
    .select(
        "is_skipped",
        "listener_id",
        F.coalesce(F.col("source"), F.lit("ALL")).alias("source"),
        "grouping_level",
        "total_ms_played_share_of_is_skipped",
    )
    .orderBy(
        F.col("is_skipped").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
        F.col("source").asc_nulls_last(),
    )
)
