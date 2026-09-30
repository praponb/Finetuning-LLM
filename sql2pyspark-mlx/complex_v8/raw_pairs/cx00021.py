from pyspark.sql import functions as F

scoped = (
    df_students
    .filter(~F.col("cohort").isin("C2022", "C2024"))
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.date_trunc("week", F.col("enrolled_on")).alias("enrolled_on_week"),
        F.concat(F.col("major"), F.lit("#"), F.col("id").cast("string")).alias("major_ref"),
        F.when(F.col("cohort").isin("C2023", "C2022"), "group_a").when(F.col("cohort") == "C2024", "group_b").otherwise("other").alias("cohort_group"),
    )
)

labeled = (
    scoped
    .select(
        "*",
        F.date_trunc("day", F.col("enrolled_on_week")).alias("enrolled_on_week_day"),
        F.year(F.col("enrolled_on_week")).alias("enrolled_on_week_year"),
    )
)

result = (
    labeled
    .groupingSets(
        [["enrolled_on_week_year", "cohort"], ["enrolled_on_week_year"]],
        "enrolled_on_week_year", "cohort",
    )
    .agg(F.array_sort(F.collect_set("cohort")).alias("cohort_list"))
    .orderBy(
        F.col("enrolled_on_week_year").asc_nulls_last(),
        F.col("cohort").asc_nulls_last(),
    )
)
