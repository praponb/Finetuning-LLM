from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_running = Window.orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_student_id = Window.partitionBy("grade_status", "student_id")

scoped = (
    df_enrollments
    .filter((F.col("course_id") >= 2) | (F.col("student_id") <= 2))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.max("score").alias("max_enrollment_score"),
    )
)

with_stats = (
    scoped.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at", "e.max_enrollment_score")
)

with_trends = (
    with_stats
    .select(
        "id",
        "course_id",
        "term",
        "grade_status_len",
        "last_enrollment_enrolled_at",
        "max_enrollment_score",
        F.first("enrolled_at", ignorenulls=True).over(w_all_by_score_running).alias("first_known_enrolled_at"),
        F.min("grade_status_len").over(w_grade_status_student_id).alias("grade_status_student_id_min_grade_status_len"),
    )
)

enriched_course = (
    with_trends.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.course_id",
        "s.grade_status_len",
        "s.max_enrollment_score",
        "s.grade_status_student_id_min_grade_status_len",
        F.col("c.level").alias("course_level"),
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
    )
)

tagged = (
    enriched_course
    .select(
        "*",
        F.concat_ws(": ", F.col("course_level"), F.col("course_dept")).alias("course_level_course_dept_label"),
        F.upper(F.trim(F.col("course_level"))).alias("course_level_clean"),
        F.coalesce(F.col("grade_status_len"), F.lit(0)).alias("grade_status_len_filled"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.count("*").alias("enrollment_count"))
)

summarized_rows = (
    tagged.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

detail_rows = (
    summarized_rows.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.course_id",
        "s.max_enrollment_score",
        "s.course_level",
        "s.course_dept",
        "s.course_level_course_dept_label",
        "s.course_level_clean",
        "s.enrollment_count",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "course_id",
        "max_enrollment_score",
        "course_level_course_dept_label",
        "course_level_clean",
        "enrollment_count",
        "enrollment_id",
        "enrollment_grade_status",
        "enrollment_score",
        F.substring(F.trim(F.col("course_dept")), 1, 3).alias("course_dept_prefix"),
    )
    .orderBy(F.col("max_enrollment_score").desc(), "id")
)
