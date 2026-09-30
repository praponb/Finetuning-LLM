from pyspark.sql import functions as F

prepared_listeners = (
    df_listeners
    .filter((F.col("birth_year") <= 1988) & (~F.array_contains(F.col("interests"), "indie")))
    .select(
        "id",
        "username",
        "signup_date",
        "birth_year",
        "interests",
        (F.floor(F.col("birth_year") / 10) * 10).alias("birth_year_bucket"),
        F.abs(F.col("birth_year") - 1981).alias("birth_year_distance"),
    )
)

enriched = (
    prepared_listeners
    .filter(F.col("signup_date").isNotNull())
    .select(
        "id",
        "username",
        "signup_date",
        "birth_year",
        "interests",
        "birth_year_bucket",
        "birth_year_distance",
        F.upper(F.trim(F.col("username"))).alias("username_clean"),
        F.size(F.col("interests")).alias("interests_count"),
    )
)

qualified = (
    enriched
    .crossJoin(enriched.agg(F.max("birth_year").alias("overall_max_birth_year")))
    .filter(F.col("birth_year") < F.col("overall_max_birth_year"))
    .drop("overall_max_birth_year")
)

result = (
    qualified
    .select("birth_year")
    .distinct()
    .orderBy(F.col("birth_year").desc())
)
