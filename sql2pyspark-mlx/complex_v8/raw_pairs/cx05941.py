from pyspark.sql import functions as F

scoped = (
    df_students
    .filter(~F.col("is_international"))
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        F.concat_ws(": ", F.col("major"), F.col("student_name")).alias("major_student_name_label"),
        F.concat(F.col("cohort"), F.lit("#"), F.col("id").cast("string")).alias("cohort_ref"),
        F.upper(F.trim(F.col("major"))).alias("major_clean"),
    )
)

grouped = (
    scoped
    .groupBy("major", "cohort")
    .agg(
        F.array_sort(F.collect_set("cohort")).alias("cohort_list"),
        F.sum(F.when(F.col("cohort").isNotNull(), 1).otherwise(0)).alias("n_cohort_is_not_null"),
        F.count("*").alias("n_rows"),
        F.countDistinct("major").alias("distinct_majors"),
        F.countDistinct("cohort").alias("distinct_cohorts"),
    )
    .filter(F.col("n_rows") >= 2)
)

screened = (
    grouped
    .crossJoin(grouped.agg(F.max("n_cohort_is_not_null").alias("overall_max_n_cohort_is_not_null")))
    .filter(F.col("n_cohort_is_not_null") < F.col("overall_max_n_cohort_is_not_null"))
    .drop("overall_max_n_cohort_is_not_null")
)

screened_2 = (
    screened
    .crossJoin(screened.agg(F.max("n_cohort_is_not_null").alias("overall_max_n_cohort_is_not_null")))
    .filter(F.col("n_cohort_is_not_null") < F.col("overall_max_n_cohort_is_not_null"))
    .drop("overall_max_n_cohort_is_not_null")
)

derived = (
    screened_2
    .select(
        "major",
        "cohort",
        "cohort_list",
        "n_cohort_is_not_null",
        "n_rows",
        "distinct_majors",
        F.length(F.trim(F.col("cohort"))).alias("cohort_len"),
    )
)

screened_3 = (
    derived
    .crossJoin(derived.agg(F.min("cohort_len").alias("overall_min_cohort_len")))
    .filter(F.col("cohort_len") > F.col("overall_min_cohort_len"))
    .drop("overall_min_cohort_len")
)

result = (
    screened_3
    .select(
        "major",
        "cohort",
        "cohort_list",
        "n_cohort_is_not_null",
        "cohort_len",
        F.size(F.col("cohort_list")).alias("cohort_list_count"),
    )
)
