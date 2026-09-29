from pyspark.sql import functions as F

filtered_listeners = (
    df_listeners
    .filter((F.col("signup_date").between(F.lit("2024-08-01").cast("date"), F.lit("2025-09-26").cast("date"))) | (F.col("birth_year") <= 1980))
)

tagged = (
    filtered_listeners
    .filter(F.col("birth_year") >= 1967)
    .select(
        "*",
        F.date_trunc("year", F.col("signup_date")).alias("signup_date_year"),
        F.abs(F.col("birth_year") - 1980).alias("birth_year_distance"),
        F.when(F.col("birth_year") >= 1990, "A").when(F.col("birth_year") >= 1975, "B").otherwise("C").alias("birth_year_tier"),
    )
)

labeled = (
    tagged
    .filter(F.col("plan").isin("premium", "student", "free"))
    .select(
        "id",
        "username",
        "country",
        "signup_date_year",
        "birth_year_distance",
        "birth_year_tier",
        F.substring(F.trim(F.col("birth_year_tier")), 1, 3).alias("birth_year_tier_prefix"),
        F.substring(F.trim(F.col("country")), 1, 4).alias("country_prefix"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("signup_date_year")).alias("days_since_signup_date_year"),
    )
)

tagged_2 = (
    labeled
    .select(
        "*",
        F.substring(F.trim(F.col("birth_year_tier")), 1, 3).alias("birth_year_tier_prefix_2"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("signup_date_year")).alias("days_since_signup_date_year_2"),
        F.concat(F.col("birth_year_tier_prefix"), F.lit("#"), F.col("birth_year_distance").cast("string")).alias("birth_year_tier_prefix_ref"),
    )
)

result = (
    tagged_2
    .groupBy("signup_date_year", "country")
    .pivot("birth_year_tier", ["A", "B", "C"])
    .agg(F.min("birth_year_distance"))
    .orderBy("signup_date_year", "country")
)
