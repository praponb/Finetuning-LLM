from pyspark.sql import functions as F

cleaned_listeners = df_listeners.filter(F.col("signup_date").isNotNull())

annotated = (
    cleaned_listeners
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        F.abs(F.col("birth_year") - 1977).alias("birth_year_distance"),
    )
)

rollup_base = (
    annotated
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_date_ym"))
    .agg(
        F.array_sort(F.collect_set("username")).alias("username_list"),
        F.sum("birth_year_distance").alias("total_birth_year_distance"),
        F.max("birth_year").alias("max_birth_year_check"),
    )
    .filter(F.col("max_birth_year_check") > 1970)
    .drop("max_birth_year_check")
)

result = (
    rollup_base
    .select(
        "signup_date_ym",
        "total_birth_year_distance",
        F.initcap(F.trim(F.col("signup_date_ym"))).alias("signup_date_ym_clean"),
    )
    .orderBy("signup_date_ym_clean", F.col("signup_date_ym").asc_nulls_last())
)
