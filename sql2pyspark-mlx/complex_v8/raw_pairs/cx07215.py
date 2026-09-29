from pyspark.sql import functions as F

base = (
    df_listeners
    .filter(F.col("plan") == "family")
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.lower(F.regexp_replace(F.trim(F.col("plan")), " ", "_")).alias("plan_slug"),
        F.date_trunc("year", F.col("signup_date")).alias("signup_date_year"),
        F.coalesce(F.col("birth_year"), F.lit(0)).alias("birth_year_filled"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.count("*").alias("play_count"),
    )
)

profiled = (
    base.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

stats = (
    profiled
    .groupBy(F.year("signup_date_year").alias("signup_date_year_year"))
    .agg(
        F.min("birth_year").alias("min_birth_year"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("username")).alias("username_list"),
        F.max("play_is_skipped_count").alias("max_play_is_skipped_count"),
        F.max("play_count").alias("max_play_count"),
    )
)

filtered = (
    stats
    .crossJoin(stats.agg(F.min("max_play_is_skipped_count").alias("overall_min_max_play_is_skipped_count")))
    .filter(F.col("max_play_is_skipped_count") > F.col("overall_min_max_play_is_skipped_count"))
    .drop("overall_min_max_play_is_skipped_count")
)

result = (
    filtered
    .select(
        "signup_date_year_year",
        "min_birth_year",
        "distinct_ids",
        "username_list",
        "max_play_is_skipped_count",
    )
    .orderBy(
        F.col("min_birth_year").asc(),
        F.col("max_play_is_skipped_count").asc_nulls_last(),
    )
)
