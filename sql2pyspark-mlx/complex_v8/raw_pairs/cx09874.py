from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_course_id_by_last_enrol = Window.partitionBy("term", "course_id").orderBy(F.col("last_enrollment_enrolled_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_student_id_last_enrollment_e = Window.partitionBy("student_id", F.month("last_enrollment_enrolled_at"))
w_term_course_id_by_last_enrol_2 = Window.partitionBy("term", "course_id").orderBy(F.col("last_enrollment_enrolled_at").desc(), "id")

base_enrollments = (
    df_enrollments
    .filter(F.col("grade_status") == "PASS")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

summarized_rows = (
    base_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

metrics = (
    summarized_rows
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "last_enrollment_enrolled_at",
        F.round(F.avg("score").over(w_term_course_id_by_last_enrol), 2).alias("score_avg_3"),
        F.min("score").over(w_student_id_last_enrollment_e).alias("student_id_last_enrollment_enrolled_at_month_min"),
        F.max("score").over(w_student_id_last_enrollment_e).alias("student_id_last_enrollment_enrolled_at_month_max"),
        (F.col("score") - F.lag("score").over(w_term_course_id_by_last_enrol_2)).alias("score_change"),
        F.lead("score", 3, 0).over(w_term_course_id_by_last_enrol_2).alias("next3_score"),
    )
)

with_parent_course = (
    metrics.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.upper(F.trim(F.col("c.course_code"))).like("C%"))
    .select(
        "s.id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.student_id_last_enrollment_enrolled_at_month_min",
        "s.student_id_last_enrollment_enrolled_at_month_max",
        "c.course_code",
    )
)

result = (
    with_parent_course
    .groupingSets(
        [["term", "course_id"], ["term"], ["course_id"]],
        "term", "course_id",
    )
    .agg(
        F.countDistinct("grade_status").alias("distinct_grade_status"),
        F.max("student_id_last_enrollment_enrolled_at_month_max").alias("max_student_id_last_enrollment_enrolled_at_month"),
    )
)
