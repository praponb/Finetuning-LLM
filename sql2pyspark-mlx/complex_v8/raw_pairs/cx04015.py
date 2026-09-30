from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_student_id = Window.partitionBy("grade_status", "student_id")
w_all_by_score = Window.orderBy("score", "id")
w_all_by_score_ties = Window.orderBy("score")
w_term_by_score = Window.partitionBy("term").orderBy("score", "id")
w_all_by_enrolled_at_prior_1 = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-1, -1)
w_student_id_term_by_enrolled = Window.partitionBy("student_id", "term").orderBy(F.col("enrolled_at").desc(), "id")
w_course_id_grade_status_by_en = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_enrollments = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
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

windowed = (
    recent_enrollments
    .select(
        "*",
        F.size(F.collect_set("term").over(w_grade_status_student_id)).alias("distinct_term_in_grade_status_student_id"),
        F.min("score").over(w_grade_status_student_id).alias("grade_status_student_id_min_score"),
        F.row_number().over(w_all_by_score).alias("rn"),
        F.round(F.cume_dist().over(w_all_by_score_ties), 3).alias("score_cume_dist"),
        (F.col("score") - F.lag("score").over(w_term_by_score)).alias("score_change"),
        F.max("score").over(w_all_by_enrolled_at_prior_1).alias("score_max_1_prior"),
        F.lag("score", 1, 0).over(w_student_id_term_by_enrolled).alias("prev_score"),
        F.first("score").over(w_course_id_grade_status_by_en).alias("first_score"),
    )
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "distinct_term_in_grade_status_student_id",
        "grade_status_student_id_min_score",
        "score_cume_dist",
        "score_max_1_prior",
        "prev_score",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.abs(F.col("score") - 34.2).alias("score_distance"),
    )
    .orderBy(F.col("days_since_enrolled_at").asc_nulls_last(), "term", "id")
    .limit(10)
)
