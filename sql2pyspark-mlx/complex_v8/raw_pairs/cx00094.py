from pyspark.sql import functions as F

cleaned = (
    df_listeners
    .filter((F.col("plan") == "premium") & (F.col("country") == "BR"))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.concat(F.col("country"), F.lit("#"), F.col("birth_year").cast("string")).alias("country_ref"),
    )
)

tagged = (
    cleaned
    .select(
        "id",
        "signup_date",
        "interests",
        F.month(F.col("signup_date")).alias("signup_date_month"),
        F.when(F.col("birth_year") >= 1990, "large").when(F.col("birth_year") >= 1975, "medium").otherwise("small").alias("birth_year_tier"),
        F.dayofweek(F.col("signup_date")).alias("signup_date_dayofweek"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.count("*").alias("play_count"))
)

profiled = (
    tagged.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

result = (
    profiled
    .groupingSets(
        [["signup_date_dayofweek", "birth_year_tier", "play_count"], ["signup_date_dayofweek"], ["birth_year_tier"], []],
        "signup_date_dayofweek", "birth_year_tier", "play_count",
    )
    .agg(
        F.round(F.sum("play_count") / F.nullif(F.sum("signup_date_month"), F.lit(0)), 3).alias("play_count_to_signup_date_month_ratio"),
        F.round(F.stddev("signup_date_dayofweek"), 2).alias("stddev_signup_date_dayofweek"),
    )
)
