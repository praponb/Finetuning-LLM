from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_credits_by_last_enrol = Window.partitionBy("course_credits").orderBy("last_enrollment_enrolled_at")
w_enrollment_course_id_2_cours = Window.partitionBy("enrollment_course_id_2", "course_level").orderBy("last_enrollment_enrolled_at", "id", "enrollment_id").rowsBetween(-2, -1)

valid = (
    df_students
    .filter(~F.col("cohort").isin("C2023", "C2021"))
    .select(
        "id",
        "student_name",
        "cohort",
        "is_international",
    )
)

annotated = (
    valid
    .select(
        "*",
        F.concat_ws("|", F.col("student_name"), F.col("cohort")).alias("student_name_cohort_label"),
        F.coalesce(F.col("cohort"), F.lit("UNKNOWN")).alias("cohort_or_unknown"),
        F.substring(F.trim(F.col("cohort")), 1, 2).alias("cohort_prefix"),
    )
)

exploded_detail_enrollments = (
    annotated.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.student_name",
        "s.cohort",
        "s.is_international",
        "s.student_name_cohort_label",
        "s.cohort_or_unknown",
        "s.cohort_prefix",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.sum("score").alias("total_enrollment_score"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

profiled = (
    exploded_detail_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.total_enrollment_score",
        "e.last_enrollment_enrolled_at",
    )
)

enriched = (
    profiled.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.enrollment_course_id"), "left")
    .filter(F.col("e.course_id") <= 4)
    .select(
        "s.*",
        F.col("e.course_id").alias("enrollment_course_id_2"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.round(F.percent_rank().over(w_course_credits_by_last_enrol), 3).alias("last_enrollment_enrolled_at_percent_rank"),
        F.round(F.avg("enrollment_count").over(w_enrollment_course_id_2_cours), 2).alias("enrollment_count_avg_2_prior"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["course_credits", "cohort_or_unknown"], ["course_credits"], ["cohort_or_unknown"], []],
        "course_credits", "cohort_or_unknown",
    )
    .agg(
        F.max("last_enrollment_enrolled_at").alias("last_last_enrollment_enrolled_at"),
    )
    .orderBy(
        F.col("course_credits").asc_nulls_last(),
        F.col("cohort_or_unknown").asc_nulls_last(),
    )
)
