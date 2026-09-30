from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_filled = Window.orderBy(F.col("score_filled").desc(), "id")
w_enrollment_grade_status_coun = Window.partitionBy("enrollment_grade_status_count", "course_id")
w_course_id_by_enrollment_coun = Window.partitionBy("course_id").orderBy("enrollment_count")
w_all_by_score_filled_running = Window.orderBy(F.col("score_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_enrollments
    .filter((F.col("course_id") <= 2) & (F.col("student_id") <= 16))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count(F.when(F.col("grade_status").isin("INCOMPLETE", "FAIL"), 1)).alias("enrollment_grade_status_count"),
    )
)

with_counts = (
    base.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

ranked = (
    with_counts
    .select(
        "*",
        F.row_number().over(w_all_by_score_filled).alias("score_filled_seq"),
        F.size(F.collect_set("grade_status").over(w_enrollment_grade_status_coun)).alias("distinct_grade_status_in_enrollment_grade_status"),
        F.ntile(5).over(w_all_by_score_filled).alias("score_filled_quintile"),
        F.round(F.percent_rank().over(w_course_id_by_enrollment_coun), 3).alias("enrollment_count_percent_rank"),
        F.count(F.lit(1)).over(w_all_by_score_filled_running).alias("running_count"),
    )
)

joined_course = (
    ranked.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.days_since_enrolled_at",
        "s.enrollment_count",
        "s.enrollment_grade_status_count",
        "s.distinct_grade_status_in_enrollment_grade_status",
        "s.enrollment_count_percent_rank",
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    joined_course
    .select(
        "id",
        "student_id",
        "grade_status",
        "enrollment_count",
        "enrollment_grade_status_count",
        "course_dept",
        "course_credits",
        F.lower(F.regexp_replace(F.trim(F.col("course_dept")), " ", "_")).alias("course_dept_slug"),
        F.substring(F.trim(F.col("grade_status")), 1, 4).alias("grade_status_prefix"),
    )
)
