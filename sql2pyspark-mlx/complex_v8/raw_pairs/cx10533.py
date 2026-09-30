from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_source = Window.partitionBy("track_id", "source")
w_listener_signup_date_month_t = Window.partitionBy(F.month("listener_signup_date"), "track_id").orderBy("listener_signup_date", "id")
w_track_id_source_by_listener = Window.partitionBy("track_id", "source").orderBy(F.col("listener_signup_date").asc_nulls_last())
w_track_id_source_by_listener_2 = Window.partitionBy("track_id", "source").orderBy(F.col("listener_signup_date").asc_nulls_last(), "id")
w_is_skipped = Window.partitionBy("is_skipped")
w_listener_plan_listener_signu = Window.partitionBy("listener_plan", F.month("listener_signup_date")).orderBy("listener_signup_date")
w_track_id_source_by_listener_3 = Window.partitionBy("track_id", "source").orderBy(F.col("listener_signup_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_plan_listener_signu_2 = Window.partitionBy("listener_plan", F.month("listener_signup_date")).orderBy("listener_signup_date", "id")
w_listener_id_track_id = Window.partitionBy("listener_id", "track_id")

scoped = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

with_parent = (
    scoped.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.plan").alias("listener_plan"),
        F.col("l.username").alias("listener_username"),
    )
)

metrics = (
    with_parent
    .select(
        "id",
        "track_id",
        "ms_played",
        "is_skipped",
        "listener_signup_date",
        "listener_username",
        F.count(F.lit(1)).over(w_track_id_source).alias("rows_in_track_id_source"),
        F.lead("ms_played").over(w_listener_signup_date_month_t).alias("next_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id_source), 2).alias("ms_played_vs_track_id_source_avg"),
        F.round(F.cume_dist().over(w_track_id_source_by_listener), 3).alias("listener_signup_date_cume_dist"),
        F.dense_rank().over(w_listener_signup_date_month_t).alias("listener_signup_date_dense_rank"),
        F.lag("listener_signup_date").over(w_track_id_source_by_listener_2).alias("prev_listener_signup_date"),
        F.count(F.lit(1)).over(w_is_skipped).alias("rows_in_is_skipped"),
        F.size(F.collect_set("track_id").over(w_is_skipped)).alias("distinct_track_id_in_is_skipped"),
        F.size(F.collect_set("listener_plan").over(w_track_id_source)).alias("distinct_listener_plan_in_track_id_source"),
        F.rank().over(w_listener_plan_listener_signu).alias("listener_signup_date_rank"),
        F.count(F.lit(1)).over(w_track_id_source_by_listener_3).alias("running_count"),
        F.row_number().over(w_listener_plan_listener_signu_2).alias("listener_signup_date_seq"),
        F.count(F.lit(1)).over(w_listener_id_track_id).alias("rows_in_listener_id_track_id"),
    )
)

enriched = (
    metrics
    .filter(F.col("next_ms_played") >= 67932)
    .select(
        "*",
        F.length(F.trim(F.col("listener_username"))).alias("listener_username_len"),
    )
)

totals = (
    enriched
    .filter(F.col("listener_signup_date_seq") <= 8)
    .groupBy(
        "listener_signup_date_rank",
        "is_skipped",
        "distinct_listener_plan_in_track_id_source",
    )
    .agg(
        F.round(100 * F.count(F.when(~F.col("is_skipped"), 1)) / F.count("*"), 1).alias("pct_is_skipped_false"),
        F.round(F.sum("next_ms_played") / F.nullif(F.sum("distinct_track_id_in_is_skipped"), F.lit(0)), 3).alias("next_ms_played_to_distinct_track_id_in_is_skippe"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("n_is_skipped_false"),
        F.sum("rows_in_track_id_source").alias("total_rows_in_track_id_source"),
        F.round(F.sum("ms_played_vs_track_id_source_avg"), 2).alias("total_ms_played_vs_track_id_source_avg"),
        F.max("rows_in_track_id_source").alias("max_rows_in_track_id_source_check"),
    )
    .filter(F.col("max_rows_in_track_id_source_check") > 9)
    .drop("max_rows_in_track_id_source_check")
)

result = (
    totals
    .filter(F.col("next_ms_played_to_distinct_track_id_in_is_skippe") < 8.0)
    .select(
        "listener_signup_date_rank",
        "distinct_listener_plan_in_track_id_source",
        "pct_is_skipped_false",
        "n_is_skipped_false",
        F.greatest(F.col("pct_is_skipped_false"), F.col("n_is_skipped_false")).alias("greatest_pct_is_skipped_false_n_is_skipped_false"),
        F.when(F.col("n_is_skipped_false") >= 6, "HIGH").when(F.col("n_is_skipped_false") >= 3, "MEDIUM").otherwise("LOW").alias("n_is_skipped_false_tier"),
    )
    .orderBy(
        F.col("pct_is_skipped_false").desc(),
        F.col("n_is_skipped_false_tier").desc_nulls_first(),
    )
)
