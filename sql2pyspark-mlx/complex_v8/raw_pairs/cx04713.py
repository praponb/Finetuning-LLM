from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status")
w_all_by_score = Window.orderBy("score", "id")
w_grade_status_by_grade_status = Window.partitionBy("grade_status").orderBy("grade_status_sum_score", "id").rowsBetween(-6, -1)
w_term_course_id_by_score_vs_g = Window.partitionBy("term", "course_id").orderBy("score_vs_grade_status_avg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_enrollments
    .filter(F.col("term").isin("2025T2", "2025T1", "2024T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.concat_ws(": ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

filtered = (
    cleaned.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
)

ranked = (
    filtered
    .select(
        "*",
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.lag("score").over(w_all_by_score).alias("prev_score"),
        F.min("score").over(w_grade_status).alias("grade_status_min_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status), F.lit(0)), 4).alias("score_share_of_grade_status"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status), 2).alias("score_vs_grade_status_avg"),
        F.sum("score").over(w_grade_status).alias("grade_status_sum_score"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
    )
)

profiled = (
    ranked.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at", "e.avg_enrollment_score")
)

with_trends = (
    profiled
    .select(
        "*",
        F.round(F.avg("grade_status_min_score").over(w_grade_status_by_grade_status), 2).alias("grade_status_min_score_avg_6_prior"),
        F.min("score_share_of_grade_status").over(w_grade_status).alias("grade_status_min_score_share_of_grade_status"),
        F.first("score_share_of_grade_status", ignorenulls=True).over(w_term_course_id_by_score_vs_g).alias("first_known_score_share_of_grade_status"),
        F.round(F.col("avg_enrollment_score") - F.avg("avg_enrollment_score").over(w_grade_status), 2).alias("avg_enrollment_score_vs_grade_status_avg"),
    )
)

with_details_course = (
    with_trends.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits") < 4), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.rows_in_grade_status",
        "s.prev_score",
        "s.grade_status_min_score",
        "s.score_share_of_grade_status",
        "s.score_vs_grade_status_avg",
        "s.grade_status_sum_score",
        "s.last_enrollment_enrolled_at",
        "s.avg_enrollment_score",
        "s.grade_status_min_score_avg_6_prior",
        "s.grade_status_min_score_share_of_grade_status",
        "s.first_known_score_share_of_grade_status",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

result = (
    with_details_course
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "rows_in_grade_status",
        "prev_score",
        "grade_status_min_score",
        "score_share_of_grade_status",
        "score_vs_grade_status_avg",
        "last_enrollment_enrolled_at",
        "avg_enrollment_score",
        "grade_status_min_score_avg_6_prior",
        "course_credits",
        "course_level",
        F.least(F.col("prev_score"), F.col("score")).alias("least_prev_score_score"),
        F.abs(F.col("grade_status_sum_score") - 350.0).alias("grade_status_sum_score_distance"),
    )
    .orderBy(F.col("grade_status_min_score").desc(), F.col("score").desc())
)
