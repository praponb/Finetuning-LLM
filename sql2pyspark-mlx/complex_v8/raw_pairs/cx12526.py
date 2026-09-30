from pyspark.sql import functions as F

cleaned_listeners = (
    df_listeners
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "interests",
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.when(F.col("birth_year") >= 1994, "HIGH").when(F.col("birth_year") >= 1968, "MEDIUM").otherwise("LOW").alias("birth_year_tier"),
        F.concat_ws("-", F.col("plan"), F.col("username")).alias("plan_username_label"),
    )
)

tagged = (
    cleaned_listeners
    .select(
        "id",
        "username",
        "country",
        "interests",
        "plan_username_label",
        F.date_format(F.col("signup_date"), "yyyy-MM").alias("signup_date_label"),
    )
)

result = (
    tagged
    .filter(F.length(F.trim(F.col("username"))) >= 7)
    .select(
        "id",
        "username",
        "country",
        "plan_username_label",
        "signup_date_label",
        F.concat_ws("-", F.col("signup_date_label"), F.col("country")).alias("signup_date_label_country_label"),
        F.concat_ws(" / ", F.col("plan_username_label"), F.col("signup_date_label")).alias("plan_username_label_signup_date_label_label"),
    )
    .orderBy(F.col("signup_date_label_country_label").asc_nulls_last(), "id")
    .limit(5)
)
