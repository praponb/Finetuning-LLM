from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter(F.lower(F.col("title")).like("%on%"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

filtered = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.length(F.trim(F.col("a.artist_name"))) >= 4), "left_semi")
)

rollup_base = (
    filtered
    .groupBy(F.date_trunc("month", "release_date").alias("release_date_month"))
    .agg(
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.round(F.stddev("days_since_release_date"), 2).alias("stddev_days_since_release_date"),
        F.min("days_since_release_date").alias("min_days_since_release_date"),
        F.count("*").alias("n_rows"),
        F.max("duration_sec").alias("max_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec_check"),
    )
    .filter(F.col("avg_duration_sec_check") > 183)
    .drop("avg_duration_sec_check")
)

exploded = (
    rollup_base
    .select(
        "release_date_month",
        "stddev_days_since_release_date",
        "min_days_since_release_date",
        "n_rows",
        "max_duration_sec",
        F.posexplode("title_list").alias("title_list_item_pos", "title_list_item"),
    )
)

derived = (
    exploded
    .select(
        "*",
        (F.col("release_date_month") + F.expr("INTERVAL 1 DAYS")).alias("release_date_month_plus_1d"),
    )
)

result = (
    derived
    .select(
        "release_date_month",
        "min_days_since_release_date",
        "n_rows",
        "max_duration_sec",
        "title_list_item_pos",
        "title_list_item",
        "release_date_month_plus_1d",
        (F.col("n_rows") + F.col("title_list_item_pos")).alias("n_rows_plus_title_list_item_pos"),
    )
    .orderBy(
        F.col("release_date_month").desc(),
        F.col("title_list_item").asc_nulls_last(),
    )
)
