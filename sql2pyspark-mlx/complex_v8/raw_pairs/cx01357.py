from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrollment_grade_status = Window.partitionBy("enrollment_grade_status")
w_all_by_enrollment_score_ties = Window.orderBy("enrollment_score")
w_enrollment_enrolled_at_month = Window.partitionBy(F.date_trunc("month", "enrollment_enrolled_at")).orderBy(F.col("enrollment_enrolled_at").desc(), "id", "enrollment_id")

cleaned = (
    df_students
    .filter(F.col("major").isNotNull())
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

detail_rows_enrollments = (
    cleaned.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.student_name",
        "s.cohort",
        "s.is_international",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

combined_details_enrollment = (
    detail_rows_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .select(
        "s.id",
        "s.student_name",
        "s.cohort",
        "s.is_international",
        "s.enrollment_id",
        "s.enrollment_grade_status",
        "s.enrollment_enrolled_at",
        "s.enrollment_score",
        "s.enrollment_course_id",
        F.col("e.term").alias("enrollment_term"),
    )
)

sequenced = (
    combined_details_enrollment
    .select(
        "id",
        "cohort",
        "is_international",
        "enrollment_id",
        "enrollment_enrolled_at",
        "enrollment_score",
        "enrollment_course_id",
        F.round(F.col("enrollment_score") - F.avg("enrollment_score").over(w_enrollment_grade_status), 2).alias("enrollment_score_vs_enrollment_grade_status_avg"),
        F.round(F.avg("enrollment_score").over(w_enrollment_grade_status), 2).alias("enrollment_grade_status_avg_enrollment_score"),
        F.round(F.percent_rank().over(w_all_by_enrollment_score_ties), 3).alias("enrollment_score_percent_rank"),
    )
)

joined = (
    sequenced.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.id") == F.col("s.enrollment_id")) & (F.col("e.grade_status") == "PASS"), "inner")
    .select(
        "s.id",
        "s.is_international",
        "s.enrollment_id",
        "s.enrollment_enrolled_at",
        "s.enrollment_course_id",
        "s.enrollment_grade_status_avg_enrollment_score",
        F.col("e.course_id").alias("enrollment_course_id_2"),
        F.col("e.term").alias("enrollment_term"),
    )
)

windowed = (
    joined
    .select(
        "*",
        F.lead("enrollment_enrolled_at", 3).over(w_enrollment_enrolled_at_month).alias("next3_enrollment_enrolled_at"),
        F.round((F.unix_timestamp("enrollment_enrolled_at") - F.unix_timestamp(F.lag("enrollment_enrolled_at").over(w_enrollment_enrolled_at_month))) / 60, 1).alias("minutes_since_prev_enrollment_enrolled_at"),
    )
)

result = (
    windowed
    .filter(F.col("enrollment_course_id") >= 4)
    .select(
        "id",
        "enrollment_id",
        "enrollment_enrolled_at",
        "enrollment_grade_status_avg_enrollment_score",
        "enrollment_course_id_2",
        "next3_enrollment_enrolled_at",
        F.when(F.col("is_international"), "yes").otherwise("no").alias("is_international_label"),
        F.date_trunc("year", F.col("enrollment_enrolled_at")).alias("enrollment_enrolled_at_year"),
    )
    .orderBy(
        "enrollment_enrolled_at_year",
        F.col("next3_enrollment_enrolled_at").desc(),
        "id",
        "enrollment_id",
    )
    .limit(20)
)
