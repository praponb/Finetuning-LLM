from pyspark.sql import functions as F

base_students = (
    df_students
    .filter(F.upper(F.trim(F.col("student_name"))).like("I%"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        F.date_trunc("year", F.col("enrolled_on")).alias("enrolled_on_year"),
        F.when(F.col("is_international"), 1).otherwise(0).alias("is_international_flag"),
    )
)

filtered = (
    base_students.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.course_id") >= 3) & (F.col("e.score") > 38.1), "left_anti")
)

summary = (
    filtered
    .groupBy("cohort")
    .agg(
        F.round(F.avg("is_international_flag"), 2).alias("avg_is_international_flag"),
        F.sum(F.when(F.col("major") == "CS", 1).otherwise(0)).alias("n_major_cs"),
        F.sum("is_international_flag").alias("total_is_international_flag"),
        F.max("gpa").alias("max_gpa_check"),
    )
    .filter(F.col("max_gpa_check") > 2.0)
    .drop("max_gpa_check")
)

tagged = (
    summary
    .select(
        "cohort",
        "avg_is_international_flag",
        "n_major_cs",
        F.coalesce(F.col("cohort"), F.lit("UNKNOWN")).alias("cohort_or_unknown"),
        F.length(F.trim(F.col("cohort"))).alias("cohort_len"),
    )
)

with_activity = (
    tagged
    .crossJoin(tagged.agg(F.max("n_major_cs").alias("overall_max_n_major_cs")))
    .filter(F.col("n_major_cs") < F.col("overall_max_n_major_cs"))
    .drop("overall_max_n_major_cs")
)

result = (
    with_activity
    .select("cohort", "cohort_or_unknown", "cohort_len")
    .orderBy(F.col("cohort_or_unknown").asc_nulls_last())
)
