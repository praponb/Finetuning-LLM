from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy("score", "id")
w_student_id_course_id = Window.partitionBy("student_id", "course_id")
w_all_by_score_moving_6 = Window.orderBy("score", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_enrollments
    .filter((F.col("student_id") <= 13) | (F.col("enrolled_at") < F.lit("2025-02-24 13:00:00").cast("timestamp")))
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

with_windows = (
    cleaned
    .select(
        "id",
        "course_id",
        "grade_status",
        "enrolled_at",
        F.ntile(4).over(w_all_by_score).alias("score_quartile"),
        F.ntile(3).over(w_all_by_score).alias("score_tercile"),
        F.max("score").over(w_student_id_course_id).alias("student_id_course_id_max_score"),
        F.size(F.collect_set("grade_status").over(w_student_id_course_id)).alias("distinct_grade_status_in_student_id_course_id"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_score))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.sum("score").over(w_all_by_score_moving_6).alias("score_sum_7"),
    )
)

enriched = (
    with_windows.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_tercile",
        "s.student_id_course_id_max_score",
        "s.distinct_grade_status_in_student_id_course_id",
        "s.minutes_since_prev_enrolled_at",
        F.col("c.level").alias("course_level"),
        "c.course_code",
    )
)

result = (
    enriched
    .groupBy("score_tercile", "course_level")
    .pivot("grade_status", ["PASS", "INCOMPLETE"])
    .agg(F.sum("distinct_grade_status_in_student_id_course_id").alias("total"), F.count("distinct_grade_status_in_student_id_course_id").alias("n"))
)
