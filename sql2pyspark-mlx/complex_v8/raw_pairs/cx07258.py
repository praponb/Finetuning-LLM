from pyspark.sql import functions as F

prepared = (
    df_listeners
    .filter(~F.array_contains(F.col("interests"), "jazz"))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
    )
)

tagged = (
    prepared
    .select(
        "*",
        F.abs(F.col("birth_year") - 1980).alias("birth_year_distance"),
        F.length(F.trim(F.col("username"))).alias("username_len"),
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)

result = (
    tagged
    .rollup("plan", "country", "country_or_unknown")
    .agg(
        F.count(F.when(F.col("plan").isNotNull(), 1)).alias("n_plan_is_not_null"),
        F.sum(F.when(F.col("country").isin("TH", "KR", "BR"), 1).otherwise(0)).alias("n_country_in_th_kr_br"),
        F.max("signup_date").alias("last_signup_date"),
    )
    .orderBy(
        F.col("plan").asc_nulls_last(),
        F.col("country").asc_nulls_last(),
        F.col("country_or_unknown").asc_nulls_last(),
    )
)
