from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id")
w_ms_played_tier_by_listener_s = Window.partitionBy("ms_played_tier").orderBy("listener_signup_date", "id")
w_all_by_listener_birth_year_p = Window.orderBy("listener_birth_year", "id").rowsBetween(-1, -1)
w_listener_country_ms_played_t = Window.partitionBy("listener_country", "ms_played_tier").orderBy("ms_played")
w_all_by_listener_birth_year_r = Window.orderBy("listener_birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_plays
    .filter(F.col("source").isin("album", "search", "playlist"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 287769, "A").when(F.col("ms_played") >= 95960, "B").otherwise("C").alias("ms_played_tier"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

combined_details = (
    valid.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.ms_played_tier",
        "s.days_since_played_at",
        F.col("l.country").alias("listener_country"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("t.title").alias("track_title"),
    )
)

ranked = (
    combined_details
    .select(
        "*",
        F.size(F.collect_set("ms_played_tier").over(w_track_id)).alias("distinct_ms_played_tier_in_track_id"),
        F.lag("listener_signup_date", 2).over(w_ms_played_tier_by_listener_s).alias("prev2_listener_signup_date"),
        F.sum("days_since_played_at").over(w_all_by_listener_birth_year_p).alias("days_since_played_at_sum_1_prior"),
        F.rank().over(w_listener_country_ms_played_t).alias("ms_played_rank"),
        F.first("listener_birth_year", ignorenulls=True).over(w_all_by_listener_birth_year_r).alias("first_known_listener_birth_year"),
        F.sum("ms_played").over(w_track_id).alias("track_id_sum_ms_played"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ms_played_rank",
        "days_since_played_at",
        "ms_played",
        "distinct_ms_played_tier_in_track_id",
    )
    .unpivot(
        ids=["id"],
        values=["ms_played_rank", "days_since_played_at", "ms_played", "distinct_ms_played_tier_in_track_id"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "measure")
)
