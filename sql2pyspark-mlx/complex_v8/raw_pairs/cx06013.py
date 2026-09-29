from pyspark.sql import functions as F

valid_listeners = (
    df_listeners
    .filter((F.col("signup_date") >= F.lit("2024-08-02").cast("date")) & (F.size(F.col("interests")) >= 1))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.add_months(F.col("signup_date"), 1).alias("signup_date_plus_1m"),
        F.upper(F.trim(F.col("country"))).alias("country_clean"),
        F.initcap(F.trim(F.col("country"))).alias("country_clean_2"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.min("played_at").alias("first_played_at_2"),
    )
)

profiled = (
    valid_listeners.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select("s.*", "p.first_played_at", "p.first_played_at_2")
)

country_stats = (
    profiled
    .groupBy("country")
    .agg(F.max("birth_year").alias("max_birth_year"))
)

compared = (
    profiled.alias("s")
    .join(country_stats.alias("g"), F.col("g.country") == F.col("s.country"), "inner")
    .filter(F.col("s.birth_year") == F.col("g.max_birth_year"))
    .select(
        "s.*",
        "g.max_birth_year",
        F.round(F.col("s.birth_year") - F.col("g.max_birth_year"), 2).alias("birth_year_minus_max"),
    )
)

annotated = (
    compared
    .select(
        "id",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        "signup_date_plus_1m",
        "country_clean_2",
        "first_played_at",
        "max_birth_year",
        "birth_year_minus_max",
        F.upper(F.trim(F.col("username"))).alias("username_clean"),
        F.round((F.unix_timestamp(F.col("first_played_at_2")) - F.unix_timestamp(F.col("first_played_at"))) / 3600, 1).alias("hours_first_played_at_to_first_played_at_2"),
        F.month(F.col("signup_date")).alias("signup_date_month"),
    )
)

result = (
    annotated
    .groupingSets(
        [["country", "plan", "signup_date_month"], ["country"], ["plan"], []],
        "country", "plan", "signup_date_month",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count(F.when(~F.col("country").isin("UK", "TH"), 1)).alias("n_country_not_in_uk_th"),
    )
    .select(
        F.coalesce(F.col("country"), F.lit("ALL")).alias("country"),
        F.coalesce(F.col("plan"), F.lit("ALL")).alias("plan"),
        "signup_date_month",
        "grouping_level",
        "n_country_not_in_uk_th",
    )
)
