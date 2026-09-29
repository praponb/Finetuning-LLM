from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_or_unknown_term_by_scor = Window.partitionBy("term_or_unknown", "term").orderBy(F.col("score").desc(), "id")
w_term_or_unknown_course_id_by = Window.partitionBy("term_or_unknown", "course_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrollment_id_enrollment_ter = Window.partitionBy("enrollment_id", "enrollment_term").orderBy(F.col("enrolled_at").desc(), "id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrollment_id_enrollment_ter_2 = Window.partitionBy("enrollment_id", "enrollment_term").orderBy(F.col("enrolled_at").desc(), "id", "enrollment_id")

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("score") < 79.3) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.concat_ws("|", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

ranked = (
    cleaned_enrollments
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "enrolled_at",
        "term_grade_status_label",
        "term_or_unknown",
        F.ntile(3).over(w_term_or_unknown_term_by_scor).alias("score_tercile"),
        F.last("score").over(w_term_or_unknown_course_id_by).alias("last_score"),
    )
)

annotated = (
    ranked
    .filter(F.col("term_or_unknown") != "2024T2")
    .select(
        "*",
        F.substring(F.trim(F.col("term_grade_status_label")), 1, 3).alias("term_grade_status_label_prefix"),
    )
)

detail_rows_enrollments = (
    annotated.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.grade_status") == "INCOMPLETE"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.term_grade_status_label",
        "s.score_tercile",
        "s.term_grade_status_label_prefix",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

with_details = (
    detail_rows_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("e.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.term_grade_status_label",
        "s.term_grade_status_label_prefix",
        "s.enrollment_id",
        "s.enrollment_term",
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at_2"),
        F.col("e.grade_status").alias("enrollment_grade_status_2"),
        F.col("c.level").alias("course_level"),
    )
)

with_trends = (
    with_details
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "enrollment_id",
        "enrollment_term",
        "enrollment_course_id",
        "enrollment_enrolled_at_2",
        "enrollment_grade_status_2",
        "course_level",
        F.last("enrollment_enrolled_at_2", ignorenulls=True).over(w_enrollment_id_enrollment_ter).alias("last_known_enrollment_enrolled_at_2"),
        F.lag("enrollment_enrolled_at_2").over(w_enrollment_id_enrollment_ter_2).alias("prev_enrollment_enrolled_at_2"),
        F.lag("enrollment_enrolled_at_2", 2).over(w_enrollment_id_enrollment_ter_2).alias("prev2_enrollment_enrolled_at_2"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "enrollment_id",
        "enrollment_term",
        "enrollment_course_id",
        "last_known_enrollment_enrolled_at_2",
        "prev2_enrollment_enrolled_at_2",
    )
    .orderBy(F.col("enrolled_at").desc_nulls_first(), "enrollment_id")
)
