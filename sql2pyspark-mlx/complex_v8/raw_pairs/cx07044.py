from pyspark.sql import functions as F

valid_listeners = (
    df_listeners
    .filter(F.col("birth_year") > 1969)
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

filtered = (
    valid_listeners.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left_semi")
)

enriched = (
    filtered
    .filter(F.col("birth_year") <= 1994)
    .select(
        "*",
        F.month(F.col("signup_date")).alias("signup_date_month"),
        F.date_format(F.col("signup_date"), "yyyy-MM-dd").alias("signup_date_label"),
    )
)

result = (
    enriched
    .groupingSets(
        [["country", "plan"], ["country"], ["plan"]],
        "country", "plan",
    )
    .agg(
        F.sum("birth_year").alias("total_birth_year"),
        F.array_sort(F.collect_set("username")).alias("username_list"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
    .orderBy(F.col("country").asc_nulls_last(), F.col("plan").asc_nulls_last())
)
