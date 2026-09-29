from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country = Window.partitionBy("country")

scoped = (
    df_listeners
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.substring(F.trim(F.col("plan")), 1, 2).alias("plan_prefix"),
    )
)

flattened = (
    scoped
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "country_or_unknown",
        "plan_prefix",
        F.explode_outer("interests").alias("interest"),
    )
)

country_stats = (
    flattened
    .groupBy("country")
    .agg(F.percentile_approx("birth_year", 0.5).alias("median_birth_year"))
)

above_average = (
    flattened.alias("s")
    .join(country_stats.alias("g"), F.col("g.country") == F.col("s.country"), "inner")
    .filter(F.col("s.birth_year") >= F.col("g.median_birth_year"))
    .select(
        "s.*",
        "g.median_birth_year",
        F.round(F.col("s.birth_year") - F.col("g.median_birth_year"), 2).alias("birth_year_minus_median"),
    )
)

ranked = (
    above_average
    .select(
        "*",
        F.round(F.col("median_birth_year") / F.nullif(F.sum("median_birth_year").over(w_country), F.lit(0)), 4).alias("median_birth_year_share_of_country"),
        F.sum("birth_year_minus_median").over(w_country).alias("country_sum_birth_year_minus_median"),
    )
)

result = (
    ranked
    .filter(F.col("plan") == "premium")
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "plan_prefix",
        "interest",
        "median_birth_year",
        "birth_year_minus_median",
        F.when(F.col("birth_year") >= 1990, "HIGH").when(F.col("birth_year") >= 1969, "MEDIUM").otherwise("LOW").alias("birth_year_tier"),
    )
    .orderBy(
        "birth_year_tier",
        F.col("median_birth_year").desc(),
        "id",
        "interest",
    )
    .limit(5)
)
