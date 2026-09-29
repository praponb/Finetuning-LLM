from pyspark.sql import functions as F

cleaned_listeners = (
    df_listeners
    .filter((F.col("birth_year") >= 1975) & (F.col("plan").isin("free", "family", "premium")))
    .select(
        "id",
        "username",
        "plan",
        "signup_date",
        "birth_year",
        "interests",
        F.last_day(F.col("signup_date")).alias("signup_date_month_end"),
        F.substring(F.trim(F.col("username")), 1, 2).alias("username_prefix"),
    )
)

exploded = (
    cleaned_listeners
    .select(
        "id",
        "username",
        "plan",
        "signup_date",
        "birth_year",
        "signup_date_month_end",
        "username_prefix",
        F.explode("interests").alias("interest"),
    )
)

derived = (
    exploded
    .select(
        "*",
        F.length(F.trim(F.col("username_prefix"))).alias("username_prefix_len"),
        F.concat_ws("-", F.col("interest"), F.col("username")).alias("interest_username_label"),
    )
)

result = (
    derived
    .groupBy("interest")
    .pivot("plan", ["free", "family", "premium"])
    .agg(F.count("username_prefix_len"))
    .orderBy("interest")
)
