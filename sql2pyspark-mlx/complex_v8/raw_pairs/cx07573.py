from pyspark.sql import functions as F

scoped_listeners = (
    df_listeners
    .filter(F.col("country").isin("JP", "TH"))
    .select(
        "id",
        "username",
        "signup_date",
        "birth_year",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("signup_date")).alias("days_since_signup_date"),
        F.concat_ws("-", F.col("username"), F.col("country")).alias("username_country_label"),
    )
)

labeled = (
    scoped_listeners
    .filter(F.col("birth_year") > 1969)
    .select(
        "*",
        F.trunc(F.col("signup_date"), "MM").alias("signup_date_month_start"),
        F.length(F.trim(F.col("username_country_label"))).alias("username_country_label_len"),
        F.length(F.trim(F.col("username_country_label"))).alias("username_country_label_len_2"),
    )
)

qualified = (
    labeled
    .crossJoin(labeled.agg(F.min("days_since_signup_date").alias("overall_min_days_since_signup_date")))
    .filter(F.col("days_since_signup_date") > F.col("overall_min_days_since_signup_date"))
    .drop("overall_min_days_since_signup_date")
)

result = (
    qualified
    .select(
        "id",
        "username",
        "birth_year",
        "days_since_signup_date",
        "signup_date_month_start",
        "username_country_label_len",
    )
)
