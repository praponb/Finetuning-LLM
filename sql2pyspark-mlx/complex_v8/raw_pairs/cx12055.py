from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrollment_course_id_by_enro = Window.partitionBy("enrollment_course_id").orderBy("enrolled_at", "id", "enrollment_id")
w_enrollment_course_id_by_enro_2 = Window.partitionBy("enrollment_course_id").orderBy("enrolled_at", "id", "enrollment_id").rowsBetween(-1, -1)
w_enrollment_id_enrollment_gra = Window.partitionBy("enrollment_id", "enrollment_grade_status")

valid = (
    df_enrollments
    .filter(F.col("student_id") >= 3)
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
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("grade_status") == "FAIL", 1)).alias("enrollment_grade_status_count"),
    )
)

with_stats = (
    valid.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

with_children_enrollments = (
    with_stats.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

with_windows = (
    with_children_enrollments
    .select(
        "*",
        F.ntile(5).over(w_enrollment_course_id_by_enro).alias("enrolled_at_quintile"),
        F.max("enrollment_score").over(w_enrollment_course_id_by_enro_2).alias("enrollment_score_max_1_prior"),
        F.lead("enrolled_at").over(w_enrollment_course_id_by_enro).alias("next_enrolled_at"),
        F.count(F.lit(1)).over(w_enrollment_id_enrollment_gra).alias("rows_in_enrollment_id_enrollment_grade_status"),
        F.size(F.collect_set("grade_status").over(w_enrollment_id_enrollment_gra)).alias("distinct_grade_status_in_enrollment_id_enrollmen"),
    )
)

result = (
    with_windows
    .groupBy("enrollment_id")
    .pivot("enrollment_grade_status", ["INCOMPLETE", "WITHDRAWN", "FAIL", "PASS"])
    .agg(F.avg("rows_in_enrollment_id_enrollment_grade_status"))
)
