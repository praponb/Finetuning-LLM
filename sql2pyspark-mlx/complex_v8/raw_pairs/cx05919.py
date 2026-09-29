from pyspark.sql import functions as F

filtered_listeners = (
    df_listeners
    .filter(F.col("plan").isin("family", "premium"))
    .select(
        "id",
        "country",
        "signup_date",
        "interests",
        F.concat(F.col("country"), F.lit("#"), F.col("birth_year").cast("string")).alias("country_ref"),
        F.when(F.col("birth_year") >= 1994, "HIGH").when(F.col("birth_year") >= 1975, "MEDIUM").otherwise("LOW").alias("birth_year_tier"),
        F.when(F.col("birth_year") >= 1988, "A").when(F.col("birth_year") >= 1975, "B").otherwise("C").alias("birth_year_tier_2"),
    )
)

enriched = (
    filtered_listeners
    .select(
        "*",
        F.substring(F.trim(F.col("birth_year_tier_2")), 1, 3).alias("birth_year_tier_2_prefix"),
        F.date_format(F.col("signup_date"), "MMM yyyy").alias("signup_date_label"),
        F.last_day(F.col("signup_date")).alias("signup_date_month_end"),
    )
)

result = (
    enriched
    .select(
        "id",
        "country",
        "signup_date",
        "interests",
        "country_ref",
        "birth_year_tier",
        "birth_year_tier_2_prefix",
        "signup_date_label",
        "signup_date_month_end",
    )
    .orderBy(F.col("birth_year_tier").desc())
)
