from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_listener_id = Window.partitionBy("source", "listener_id")
w_listener_country_is_skipped = Window.partitionBy("listener_country", "is_skipped").orderBy("listener_signup_date", "id")
w_listener_country_is_skipped_2 = Window.partitionBy("listener_country", "is_skipped").orderBy(F.col("ms_played").desc())
w_is_skipped_ms_played_tier_by = Window.partitionBy("is_skipped", "ms_played_tier").orderBy("ms_played", "id").rowsBetween(-6, -1)
w_listener_country_is_skipped_3 = Window.partitionBy("listener_country", "is_skipped").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_country_is_skipped_4 = Window.partitionBy("listener_country", "is_skipped").orderBy("listener_signup_date", "id").rowsBetween(-6, Window.currentRow)
w_listener_country_source_or_u = Window.partitionBy("listener_country", "source_or_unknown").orderBy(F.col("ms_played").asc_nulls_last(), "id")
w_is_skipped_ms_played_tier_by_2 = Window.partitionBy("is_skipped", "ms_played_tier").orderBy("ms_played", "id")

filtered = (
    df_plays
    .filter(~F.col("device").isin("tv", "phone"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 220498, "A").when(F.col("ms_played") >= 98052, "B").otherwise("C").alias("ms_played_tier"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

with_parent = (
    filtered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.ms_played_tier",
        "s.source_or_unknown",
        F.col("l.country").alias("listener_country"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

with_trends = (
    with_parent
    .select(
        "*",
        F.min("ms_played").over(w_source_listener_id).alias("source_listener_id_min_ms_played"),
        F.lag("ms_played", 3).over(w_listener_country_is_skipped).alias("prev3_ms_played"),
        F.round(F.percent_rank().over(w_listener_country_is_skipped_2), 3).alias("ms_played_percent_rank"),
        F.size(F.collect_set("source_or_unknown").over(w_source_listener_id)).alias("distinct_source_or_unknown_in_source_listener_id"),
        F.round(F.avg("ms_played").over(w_is_skipped_ms_played_tier_by), 2).alias("ms_played_avg_6_prior"),
        F.first("listener_signup_date").over(w_listener_country_is_skipped_3).alias("first_listener_signup_date"),
        F.round(F.avg("ms_played").over(w_listener_country_is_skipped_4), 2).alias("ms_played_avg_7"),
        (F.col("ms_played") - F.lag("ms_played").over(w_listener_country_source_or_u)).alias("ms_played_change"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_source_listener_id), 2).alias("ms_played_vs_source_listener_id_avg"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_source_listener_id), F.lit(0)), 4).alias("ms_played_share_of_source_listener_id"),
        F.row_number().over(w_is_skipped_ms_played_tier_by_2).alias("rn"),
    )
)

leaders = (
    with_trends
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    leaders
    .select(
        "id",
        "listener_id",
        "source",
        "ms_played_tier",
        "listener_country",
        "listener_signup_date",
        "source_listener_id_min_ms_played",
        "prev3_ms_played",
        "ms_played_percent_rank",
        "ms_played_avg_6_prior",
        "first_listener_signup_date",
        "ms_played_avg_7",
        "ms_played_change",
        "ms_played_vs_source_listener_id_avg",
        "ms_played_share_of_source_listener_id",
        F.date_add(F.col("listener_signup_date"), 30).alias("listener_signup_date_plus_30d"),
    )
    .orderBy(F.col("id").asc())
    .limit(5)
)
