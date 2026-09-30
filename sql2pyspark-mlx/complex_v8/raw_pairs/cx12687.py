from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_signup_date_month = Window.partitionBy(F.date_trunc("month", "listener_signup_date"))
w_source = Window.partitionBy("source")
w_all_by_listener_birth_year_r = Window.orderBy("listener_birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_year_ties = Window.orderBy(F.col("played_at_year").asc_nulls_last())
w_listener_id_by_listener_birt = Window.partitionBy("listener_id").orderBy(F.col("listener_birth_year").desc())
w_all_by_played_at = Window.orderBy("played_at", "id")
w_all_by_played_at_ties = Window.orderBy("played_at")
w_listener_id_by_listener_birt_2 = Window.partitionBy("listener_id").orderBy(F.col("listener_birth_year").desc(), "id")

recent = (
    df_plays
    .filter((F.col("track_id").isNotNull()) | (F.col("ms_played") >= 151112))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.year(F.col("played_at")).alias("played_at_year"),
        F.abs(F.col("ms_played") - 178909).alias("ms_played_distance"),
    )
)

joined_listener = (
    recent.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.source",
        "s.played_at_year",
        "s.ms_played_distance",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

with_windows = (
    joined_listener
    .select(
        "*",
        F.round(F.col("ms_played_distance") - F.avg("ms_played_distance").over(w_listener_signup_date_month), 2).alias("ms_played_distance_vs_listener_signup_date_month"),
        F.max("played_at_year").over(w_source).alias("source_max_played_at_year"),
        F.round(F.col("played_at_year") / F.nullif(F.sum("played_at_year").over(w_listener_signup_date_month), F.lit(0)), 4).alias("played_at_year_share_of_listener_signup_date_mon"),
        F.last("played_at_year", ignorenulls=True).over(w_all_by_listener_birth_year_r).alias("last_known_played_at_year"),
        F.min("ms_played_distance").over(w_source).alias("source_min_ms_played_distance"),
        F.round(F.percent_rank().over(w_all_by_played_at_year_ties), 3).alias("played_at_year_percent_rank"),
        F.dense_rank().over(w_listener_id_by_listener_birt).alias("listener_birth_year_dense_rank"),
        F.datediff(F.col("listener_signup_date"), F.lag("listener_signup_date").over(w_all_by_played_at)).alias("days_since_prev_listener_signup_date"),
        F.round(F.percent_rank().over(w_all_by_played_at_ties), 3).alias("played_at_percent_rank"),
        F.round(F.col("listener_birth_year") / F.nullif(F.sum("listener_birth_year").over(w_listener_signup_date_month), F.lit(0)), 4).alias("listener_birth_year_share_of_listener_signup_dat"),
        F.rank().over(w_listener_id_by_listener_birt).alias("listener_birth_year_rank"),
        F.lag("listener_birth_year", 3).over(w_all_by_played_at).alias("prev3_listener_birth_year"),
        F.round(F.percent_rank().over(w_listener_id_by_listener_birt_2), 3).alias("listener_birth_year_percent_rank"),
        F.sum("ms_played_distance").over(w_all_by_listener_birth_year_r).alias("running_ms_played_distance"),
    )
)

scored = (
    with_windows
    .filter(F.col("last_known_played_at_year") <= 2025)
    .select(
        "id",
        "track_id",
        "device",
        "played_at_year",
        "ms_played_distance",
        "listener_plan",
        "listener_signup_date",
        "listener_birth_year",
        "ms_played_distance_vs_listener_signup_date_month",
        "played_at_year_share_of_listener_signup_date_mon",
        "last_known_played_at_year",
        "source_min_ms_played_distance",
        "listener_birth_year_dense_rank",
        "days_since_prev_listener_signup_date",
        "listener_birth_year_share_of_listener_signup_dat",
        "listener_birth_year_rank",
        "listener_birth_year_percent_rank",
        F.quarter(F.col("listener_signup_date")).alias("listener_signup_date_quarter"),
        F.date_add(F.col("listener_signup_date"), 365).alias("listener_signup_date_plus_365d"),
    )
)

result = (
    scored
    .select(
        "id",
        "track_id",
        "device",
        "played_at_year",
        "ms_played_distance",
        "listener_plan",
        "listener_birth_year",
        "ms_played_distance_vs_listener_signup_date_month",
        "played_at_year_share_of_listener_signup_date_mon",
        "source_min_ms_played_distance",
        "listener_birth_year_dense_rank",
        "days_since_prev_listener_signup_date",
        "listener_birth_year_share_of_listener_signup_dat",
        "listener_birth_year_percent_rank",
        "listener_signup_date_quarter",
        "listener_signup_date_plus_365d",
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
        (F.floor(F.col("listener_birth_year") / 10) * 10).alias("listener_birth_year_bucket"),
    )
)
