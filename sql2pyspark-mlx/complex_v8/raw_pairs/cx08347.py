from pyspark.sql import functions as F

valid = (
    df_listeners
    .select(
        "id",
        "username",
        "plan",
        "birth_year",
        "interests",
        F.date_format(F.col("signup_date"), "MMM yyyy").alias("signup_date_label"),
    )
)

labeled = (
    valid
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("plan")), " ", "_")).alias("plan_slug"),
        F.coalesce(F.col("plan"), F.lit("UNKNOWN")).alias("plan_or_unknown"),
    )
)

tagged = (
    labeled
    .select(
        "*",
        F.array_contains(F.col("interests"), "kpop").alias("has_kpop"),
        F.length(F.trim(F.col("plan"))).alias("plan_len"),
        F.initcap(F.trim(F.col("username"))).alias("username_clean"),
    )
)

result = (
    tagged
    .groupBy("has_kpop")
    .pivot("plan_or_unknown", ["free", "family", "UNKNOWN", "student"])
    .agg(F.sum("birth_year"))
    .orderBy("has_kpop")
)
