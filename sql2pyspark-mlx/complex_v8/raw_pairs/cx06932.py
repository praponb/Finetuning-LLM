from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_listener_birth_year = Window.orderBy("listener_birth_year", "id")
w_played_at_month_listener_id = Window.partitionBy(F.date_trunc("month", "played_at"), "listener_id")
w_device_by_ms_played_running = Window.partitionBy("device").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_plan_by_played_at_p = Window.partitionBy("listener_plan").orderBy(F.col("played_at").desc(), "id").rowsBetween(-6, -1)
w_listener_id = Window.partitionBy("listener_id")
w_listener_plan_by_played_at = Window.partitionBy("listener_plan").orderBy(F.col("played_at").desc(), "id")

valid_plays = (
    df_plays
    .filter((F.hour(F.col("played_at")).between(9, 11)) & (F.col("listener_id").isNotNull()))
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
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.*",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

ranked = (
    with_parent
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "ms_played_filled",
        "listener_plan",
        "listener_birth_year",
        F.row_number().over(w_all_by_listener_birth_year).alias("rn"),
        F.size(F.collect_set("source").over(w_played_at_month_listener_id)).alias("distinct_source_in_played_at_month_listener_id"),
        F.sum("listener_birth_year").over(w_device_by_ms_played_running).alias("running_listener_birth_year"),
        F.count(F.lit(1)).over(w_played_at_month_listener_id).alias("rows_in_played_at_month_listener_id"),
        F.round(F.avg("ms_played_filled").over(w_listener_plan_by_played_at_p), 2).alias("ms_played_filled_avg_6_prior"),
        F.round(F.col("listener_birth_year") / F.nullif(F.sum("listener_birth_year").over(w_listener_id), F.lit(0)), 4).alias("listener_birth_year_share_of_listener_id"),
        F.ntile(10).over(w_listener_plan_by_played_at).alias("played_at_decile"),
        F.round(F.avg("ms_played_filled").over(w_played_at_month_listener_id), 2).alias("played_at_month_listener_id_avg_ms_played_filled"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "ms_played_filled",
        "listener_birth_year",
        "distinct_source_in_played_at_month_listener_id",
        "listener_birth_year_share_of_listener_id",
        "played_at_decile",
        "played_at_month_listener_id_avg_ms_played_filled",
        F.when(F.col("listener_plan").isin("free", "premium"), "group_a").when(F.col("listener_plan") == "family", "group_b").otherwise("other").alias("listener_plan_group"),
    )
    .orderBy(
        F.col("played_at_month_listener_id_avg_ms_played_filled").desc(),
        "ms_played",
    )
)
