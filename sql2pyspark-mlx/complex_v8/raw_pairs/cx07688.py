from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_month_track_id = Window.partitionBy(F.date_trunc("month", "played_at"), "track_id")
w_ms_played_tier_played_at_to = Window.partitionBy("ms_played_tier", F.to_date("played_at")).orderBy(F.col("played_at").desc())
w_ms_played_tier_played_at_to_2 = Window.partitionBy("ms_played_tier", F.to_date("played_at")).orderBy(F.col("played_at").desc(), "id")
w_all_by_ms_played = Window.orderBy(F.col("ms_played").asc_nulls_last(), "id")
w_source_listener_country = Window.partitionBy("source", "listener_country")
w_all_by_seq_no_full = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_plays = (
    df_plays
    .filter(F.col("track_id") >= 23)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
        F.when(F.col("ms_played") >= 261579, "HIGH").when(F.col("ms_played") >= 73335, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
    )
)

ranked = (
    scoped_plays
    .select(
        "*",
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_played_at_month_track_id), 2).alias("ms_played_vs_played_at_month_track_id_avg"),
        F.dense_rank().over(w_ms_played_tier_played_at_to).alias("played_at_dense_rank"),
        F.row_number().over(w_ms_played_tier_played_at_to_2).alias("played_at_seq"),
        F.row_number().over(w_all_by_ms_played).alias("seq_no"),
    )
)

combined_details = (
    ranked.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.source",
        "s.source_prefix",
        "s.ms_played_tier",
        "s.played_at_label",
        "s.ms_played_vs_played_at_month_track_id_avg",
        "s.played_at_dense_rank",
        "s.played_at_seq",
        "s.seq_no",
        F.col("l.country").alias("listener_country"),
        F.col("l.plan").alias("listener_plan"),
    )
)

with_activity = (
    combined_details.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.track_id").isNotNull()), "left_anti")
    .filter(F.col("played_at_seq") < 9)
)

joined = (
    with_activity.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.col("l.plan").alias("listener_plan_2"),
        F.col("l.interests").alias("listener_interests"),
    )
)

first_rows = joined.filter(F.col("seq_no") == 1)

with_windows = (
    first_rows
    .select(
        "*",
        F.round(F.col("seq_no") / F.nullif(F.sum("seq_no").over(w_source_listener_country), F.lit(0)), 4).alias("seq_no_share_of_source_listener_country"),
        F.first("seq_no").over(w_all_by_seq_no_full).alias("first_seq_no"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "listener_id",
        "track_id",
        "source",
        "source_prefix",
        "ms_played_tier",
        "ms_played_vs_played_at_month_track_id_avg",
        "played_at_dense_rank",
        "listener_country",
        "listener_plan_2",
        "listener_interests",
        "seq_no_share_of_source_listener_country",
        "first_seq_no",
        F.coalesce(F.col("ms_played_tier"), F.lit("UNKNOWN")).alias("ms_played_tier_or_unknown"),
        F.upper(F.trim(F.col("source_prefix"))).alias("source_prefix_clean"),
    )
    .orderBy(
        F.col("listener_id").desc(),
        F.col("source_prefix").asc_nulls_last(),
        "id",
    )
    .limit(5)
)
