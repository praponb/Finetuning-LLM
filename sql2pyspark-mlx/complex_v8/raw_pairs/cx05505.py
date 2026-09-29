from pyspark.sql import functions as F

base = (
    df_listeners
    .filter((F.col("plan") == "student") & (F.col("birth_year").between(1966, 1990)))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "birth_year",
    )
)

country_stats = (
    base
    .groupBy("country")
    .agg(F.avg("birth_year").alias("avg_birth_year"))
)

compared = (
    base.alias("s")
    .join(country_stats.alias("g"), F.col("g.country") == F.col("s.country"), "inner")
    .filter(F.col("s.birth_year") > F.col("g.avg_birth_year"))
    .select(
        "s.*",
        "g.avg_birth_year",
        F.round(F.col("s.birth_year") - F.col("g.avg_birth_year"), 2).alias("birth_year_minus_avg"),
    )
)

plan_stats = (
    compared
    .groupBy("plan")
    .agg(F.percentile_approx("birth_year_minus_avg", 0.5).alias("median_birth_year_minus_avg"))
)

vs_group = (
    compared.alias("s")
    .join(plan_stats.alias("g"), F.col("g.plan") == F.col("s.plan"), "inner")
    .filter(F.col("s.birth_year_minus_avg") >= F.col("g.median_birth_year_minus_avg"))
    .select(
        "s.*",
        "g.median_birth_year_minus_avg",
        F.round(F.col("s.birth_year_minus_avg") - F.col("g.median_birth_year_minus_avg"), 2).alias("birth_year_minus_avg_minus_median"),
    )
)

filtered = (
    vs_group
    .crossJoin(vs_group.agg(F.avg("birth_year_minus_avg").alias("overall_avg_birth_year_minus_avg")))
    .filter(F.col("birth_year_minus_avg") > F.col("overall_avg_birth_year_minus_avg"))
    .drop("overall_avg_birth_year_minus_avg")
)

totals = (
    filtered
    .groupBy("country", "plan")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

filtered_2 = (
    totals
    .crossJoin(totals.agg(F.avg("row_count").alias("overall_avg_row_count")))
    .filter(F.col("row_count") > F.col("overall_avg_row_count"))
    .drop("overall_avg_row_count")
)

screened = (
    filtered_2
    .crossJoin(filtered_2.agg(F.avg("row_count").alias("overall_avg_row_count")))
    .filter(F.col("row_count") > F.col("overall_avg_row_count"))
    .drop("overall_avg_row_count")
)

result = (
    screened
    .groupingSets(
        [["row_count", "plan", "distinct_ids"], ["row_count"], ["plan"], []],
        "row_count", "plan", "distinct_ids",
    )
    .agg(F.array_sort(F.collect_set("country")).alias("country_list"))
    .orderBy(
        F.col("row_count").asc_nulls_last(),
        F.col("plan").asc_nulls_last(),
        F.col("distinct_ids").asc_nulls_last(),
    )
)
