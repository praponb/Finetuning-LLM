from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cohort_or_unknown_major_by_m = Window.partitionBy("cohort_or_unknown", "major").orderBy(F.col("min_enrollment_score").desc(), "id")
w_first_enrollment_enrolled_at = Window.partitionBy(F.month("first_enrollment_enrolled_at"), "cohort")
w_cohort_or_unknown_major_by_m_2 = Window.partitionBy("cohort_or_unknown", "major").orderBy(F.col("min_enrollment_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_students = (
    df_students
    .filter((F.col("gpa") > 1.7) & (~F.col("is_international")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        F.coalesce(F.col("cohort"), F.lit("UNKNOWN")).alias("cohort_or_unknown"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

summarized_rows = (
    recent_students.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select("s.*", "e.min_enrollment_score", "e.first_enrollment_enrolled_at")
)

with_trends = (
    summarized_rows
    .select(
        "*",
        F.datediff(F.col("enrolled_on"), F.lag("enrolled_on").over(w_cohort_or_unknown_major_by_m)).alias("days_since_prev_enrolled_on"),
        F.max("min_enrollment_score").over(w_first_enrollment_enrolled_at).alias("first_enrollment_enrolled_at_month_cohort_max_mi"),
        F.lag("min_enrollment_score").over(w_cohort_or_unknown_major_by_m).alias("prev_min_enrollment_score"),
        F.round(F.col("min_enrollment_score") / F.nullif(F.sum("min_enrollment_score").over(w_first_enrollment_enrolled_at), F.lit(0)), 4).alias("min_enrollment_score_share_of_first_enrollment_e"),
        F.first("enrolled_on", ignorenulls=True).over(w_cohort_or_unknown_major_by_m_2).alias("first_known_enrolled_on"),
    )
)

detail_rows = (
    with_trends.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.to_date(F.col("e.enrolled_at")) < F.lit("2025-02-18").cast("date")), "inner")
    .select(
        "s.id",
        "s.student_name",
        "s.major",
        "s.enrolled_on",
        "s.min_enrollment_score",
        "s.first_enrollment_enrolled_at",
        "s.first_enrollment_enrolled_at_month_cohort_max_mi",
        "s.min_enrollment_score_share_of_first_enrollment_e",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

result = (
    detail_rows
    .rollup("enrollment_grade_status", "enrollment_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("major").alias("distinct_majors"),
        F.sum("first_enrollment_enrolled_at_month_cohort_max_mi").alias("total_first_enrollment_enrolled_at_month_cohort_"),
    )
    .select(
        F.coalesce(F.col("enrollment_grade_status"), F.lit("ALL")).alias("enrollment_grade_status"),
        "enrollment_id",
        "grouping_level",
        "distinct_majors",
        "total_first_enrollment_enrolled_at_month_cohort_",
    )
    .orderBy(
        F.col("enrollment_grade_status").asc_nulls_last(),
        F.col("enrollment_id").asc_nulls_last(),
    )
)
