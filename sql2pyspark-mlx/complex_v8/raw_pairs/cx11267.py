from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrollment_student_id = Window.partitionBy("enrollment_student_id")
w_student_id_enrollment_id_by = Window.partitionBy("student_id", "enrollment_id").orderBy(F.col("enrollment_score").desc(), "id", "enrollment_id")

base = (
    df_enrollments
    .filter((F.col("grade_status") == "FAIL") & (F.col("score") <= 57.3))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
    )
)

with_children_enrollments = (
    base.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.grade_status_clean",
        "s.grade_status_ref",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

combined_details = (
    with_children_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.grade_status_clean",
        "s.grade_status_ref",
        "s.enrollment_id",
        "s.enrollment_term",
        "s.enrollment_score",
        "s.enrollment_student_id",
        "s.enrollment_grade_status",
        F.col("st.cohort").alias("student_cohort"),
    )
)

with_trends = (
    combined_details
    .select(
        "id",
        "course_id",
        "grade_status",
        "grade_status_ref",
        "enrollment_id",
        "enrollment_score",
        "enrollment_student_id",
        "enrollment_grade_status",
        F.count(F.lit(1)).over(w_enrollment_student_id).alias("rows_in_enrollment_student_id"),
        F.lag("enrollment_score", 2, 0).over(w_student_id_enrollment_id_by).alias("prev2_enrollment_score"),
        F.round(F.col("enrollment_score") - F.avg("enrollment_score").over(w_enrollment_student_id), 2).alias("enrollment_score_vs_enrollment_student_id_avg"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "course_id",
        "enrollment_id",
        "enrollment_grade_status",
        "rows_in_enrollment_student_id",
        "prev2_enrollment_score",
        "enrollment_score_vs_enrollment_student_id_avg",
        F.round(F.col("enrollment_score") / F.nullif(F.col("prev2_enrollment_score"), F.lit(0)), 3).alias("enrollment_score_per_prev2_enrollment_score"),
    )
    .orderBy(F.col("prev2_enrollment_score").asc(), "id", "enrollment_id")
    .limit(5)
)
