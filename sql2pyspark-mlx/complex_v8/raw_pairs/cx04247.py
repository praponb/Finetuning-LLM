from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_all_by_prior_ms_played_3d_fu = Window.orderBy("prior_ms_played_3d", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_ids = Window.partitionBy("distinct_ids")

joined_ranges = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.ms_played").alias("prior_ms_played_3d"),
    )
)

with_trends = (
    joined_ranges
    .select(
        "*",
        F.round(F.col("prior_rows_3d") - F.avg("prior_rows_3d").over(w_listener_id), 2).alias("prior_rows_3d_vs_listener_id_avg"),
        F.nth_value("prior_rows_3d", 2).over(w_all_by_prior_ms_played_3d_fu).alias("second_prior_rows_3d"),
        F.round(F.col("prior_ms_played_3d") / F.nullif(F.sum("prior_ms_played_3d").over(w_listener_id), F.lit(0)), 4).alias("prior_ms_played_3d_share_of_listener_id"),
        F.first("played_at").over(w_all_by_prior_ms_played_3d_fu).alias("first_played_at"),
    )
)

per_group = (
    with_trends
    .groupBy(F.date_trunc("month", "played_at").alias("played_at_month"))
    .agg(
        F.round(F.stddev("prior_rows_3d_vs_listener_id_avg"), 2).alias("stddev_prior_rows_3d_vs_listener_id_avg"),
        F.max("prior_rows_3d").alias("max_prior_rows_3d"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("max_prior_rows_3d") > 2)
)

with_windows = (
    per_group
    .select(
        "played_at_month",
        "max_prior_rows_3d",
        "distinct_listener_ids",
        "distinct_ids",
        F.count(F.lit(1)).over(w_distinct_ids).alias("rows_in_distinct_ids"),
    )
)

result = (
    with_windows
    .select(
        "played_at_month",
        "distinct_ids",
        "rows_in_distinct_ids",
        F.round(F.col("max_prior_rows_3d") / F.nullif(F.col("distinct_ids"), F.lit(0)), 3).alias("max_prior_rows_3d_per_distinct_ids"),
        F.to_date(F.col("played_at_month")).alias("played_at_month_date"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_last())
)
