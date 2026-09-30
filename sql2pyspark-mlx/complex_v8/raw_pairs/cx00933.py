from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_ms_played_sour = Window.partitionBy("is_skipped").orderBy(F.col("ms_played_source_search").desc(), "is_skipped", "played_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_by_ms_played_sour_2 = Window.partitionBy("is_skipped").orderBy(F.col("ms_played_source_search").desc(), "is_skipped", "played_at_month")
w_all_by_ms_played_source_sear = Window.orderBy(F.col("ms_played_source_search").asc_nulls_last())
w_distinct_listener_ids = Window.partitionBy("distinct_listener_ids")

base = (
    df_plays
    .filter((F.col("is_skipped")) | (F.col("ms_played") <= 302279))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        (F.col("played_at") + F.expr("INTERVAL 7 DAYS")).alias("played_at_plus_7d"),
        F.concat(F.col("source"), F.lit("#"), F.col("id").cast("string")).alias("source_ref"),
    )
)

screened = (
    base.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.played_at") < F.lit("2025-02-24 11:00:00").cast("timestamp")), "left_anti")
)

grouped = (
    screened
    .filter(F.col("source_ref").isNotNull())
    .groupBy(
        "is_skipped",
        F.date_trunc("month", "played_at").alias("played_at_month"),
    )
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.max("played_at").alias("last_played_at"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
        F.percentile_approx("ms_played", 0.5).alias("median_ms_played"),
        F.sum(F.when(F.col("source") == "search", F.col("ms_played"))).alias("ms_played_source_search"),
    )
)

sequenced = (
    grouped
    .select(
        "*",
        F.last("first_played_at").over(w_is_skipped_by_ms_played_sour).alias("last_first_played_at"),
        F.lead("first_played_at", 3).over(w_is_skipped_by_ms_played_sour_2).alias("next3_first_played_at"),
        F.dense_rank().over(w_all_by_ms_played_source_sear).alias("ms_played_source_search_dense_rank"),
        F.sum("distinct_listener_ids").over(w_distinct_listener_ids).alias("distinct_listener_ids_sum_distinct_listener_ids"),
    )
)

result = (
    sequenced
    .select(
        "is_skipped",
        "played_at_month",
        "first_played_at",
        "last_played_at",
        "distinct_listener_ids",
        "distinct_track_ids",
        "median_ms_played",
        "ms_played_source_search",
        "last_first_played_at",
        "next3_first_played_at",
        "ms_played_source_search_dense_rank",
        "distinct_listener_ids_sum_distinct_listener_ids",
        F.round(F.col("ms_played_source_search") / F.nullif(F.col("median_ms_played"), F.lit(0)), 3).alias("ms_played_source_search_per_median_ms_played"),
    )
    .orderBy(F.col("first_played_at").desc(), F.col("last_played_at").desc())
)
