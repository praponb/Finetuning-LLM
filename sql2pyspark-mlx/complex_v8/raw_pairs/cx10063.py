from pyspark.sql import functions as F

recent_listeners = (
    df_listeners
    .filter((F.col("signup_date") < F.lit("2025-01-29").cast("date")) & (F.col("country").isNotNull()))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.concat_ws("|", F.col("country"), F.col("plan")).alias("country_plan_label"),
        F.when(F.col("birth_year") >= 1986, "A").when(F.col("birth_year") >= 1975, "B").otherwise("C").alias("birth_year_tier"),
    )
)

exploded = (
    recent_listeners
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "country_plan_label",
        "birth_year_tier",
        F.explode("interests").alias("interest"),
    )
)

result = (
    exploded
    .cube("birth_year_tier", "plan", "country")
    .agg(
        F.max("birth_year").alias("max_birth_year"),
        F.countDistinct("interest").alias("distinct_interests"),
        F.round(F.stddev("birth_year"), 2).alias("stddev_birth_year"),
    )
)
