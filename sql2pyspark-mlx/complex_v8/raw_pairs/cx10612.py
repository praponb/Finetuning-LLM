from pyspark.sql import functions as F

cleaned = (
    df_listeners
    .select(
        "id",
        "username",
        "plan",
        "signup_date",
        "birth_year",
        "interests",
    )
)

tagged = (
    cleaned
    .filter(F.upper(F.trim(F.col("username"))).like("F%"))
    .select(
        "*",
        F.upper(F.trim(F.col("plan"))).alias("plan_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("plan")), " ", "_")).alias("plan_slug"),
        F.date_add(F.col("signup_date"), 365).alias("signup_date_plus_365d"),
    )
)

annotated = (
    tagged
    .filter(F.col("signup_date") >= F.lit("2024-10-07").cast("date"))
    .select(
        "*",
        F.concat(F.col("plan"), F.lit("#"), F.col("id").cast("string")).alias("plan_ref"),
        (F.floor(F.col("birth_year") / 10) * 10).alias("birth_year_bucket"),
    )
)

result = (
    annotated
    .filter(F.col("birth_year") < 1980)
    .select(
        "id",
        "username",
        "signup_date",
        "birth_year",
        "interests",
        "plan_clean",
        "plan_slug",
        "signup_date_plus_365d",
        "plan_ref",
        "birth_year_bucket",
        F.date_format(F.col("signup_date_plus_365d"), "yyyy-MM-dd").alias("signup_date_plus_365d_label"),
    )
    .orderBy("username", "id")
    .limit(10)
)
