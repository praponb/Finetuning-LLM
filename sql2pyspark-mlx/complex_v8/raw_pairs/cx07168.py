from pyspark.sql import functions as F

in_window = (
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

with_parent = (
    in_window.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.upper(F.trim(F.col("l.username"))).like("C%")), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.prior_rows_3d",
        F.col("l.username").alias("listener_username"),
        F.col("l.country").alias("listener_country"),
    )
)

grouped = (
    with_parent
    .groupBy("listener_country")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("prior_rows_3d"), 2).alias("stddev_prior_rows_3d"),
        F.max("played_at").alias("last_played_at"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.array_sort(F.collect_set("listener_username")).alias("listener_username_list"),
    )
    .filter(F.col("cnt") >= 3)
)

result = (
    grouped
    .select(
        "listener_country",
        "cnt",
        "stddev_prior_rows_3d",
        "distinct_listener_ids",
        "listener_username_list",
        F.lower(F.regexp_replace(F.trim(F.col("listener_country")), " ", "_")).alias("listener_country_slug"),
        F.to_date(F.col("last_played_at")).alias("last_played_at_date"),
    )
    .orderBy("listener_country")
)
