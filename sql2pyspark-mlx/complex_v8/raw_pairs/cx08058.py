from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_term_group_by_enrolled = Window.partitionBy("term", "term_group").orderBy("enrolled_at", "id")
w_grade_status_by_score_adj_fu = Window.partitionBy("grade_status").orderBy(F.col("score_adj").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_grade_status_by_score_adj_ti = Window.partitionBy("grade_status").orderBy(F.col("score_adj").desc())
w_course_id = Window.partitionBy("course_id")

valid_enrollments = (
    df_enrollments
    .filter(F.col("grade_status") == "WITHDRAWN")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
        F.when(F.col("term").isin("2024T2", "2024T1"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other").alias("term_group"),
    )
)

with_windows = (
    valid_enrollments
    .select(
        "*",
        F.ntile(3).over(w_term_term_group_by_enrolled).alias("enrolled_at_tercile"),
        F.first("score_adj").over(w_grade_status_by_score_adj_fu).alias("first_score_adj"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_id_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.rank().over(w_grade_status_by_score_adj_ti).alias("score_adj_rank"),
        F.min("score").over(w_course_id).alias("course_id_min_score"),
        F.nth_value("enrolled_at", 2).over(w_grade_status_by_score_adj_fu).alias("second_enrolled_at"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.count("*").alias("enrollment_count"))
)

summarized_rows = (
    with_windows.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    summarized_rows
    .groupBy("score_adj_rank", "grade_status")
    .pivot("term_group", ["other", "group_b", "group_a"])
    .agg(F.max("first_score_adj"))
    .orderBy("score_adj_rank", "grade_status")
)
