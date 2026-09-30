from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_month_student_id = Window.partitionBy(F.month("enrolled_at"), "student_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term = Window.partitionBy("term")
w_course_id_term_by_term_sum_s = Window.partitionBy("course_id", "term").orderBy("term_sum_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_term_by_term_sum_s_2 = Window.partitionBy("course_id", "term").orderBy("term_sum_score", "id").rowsBetween(-2, Window.currentRow)

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("score") >= 38.3)
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
    cleaned_enrollments
    .select(
        "id",
        "course_id",
        "term",
        "enrolled_at",
        F.first("score").over(w_enrolled_at_month_student_id).alias("first_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_term), F.lit(0)), 4).alias("score_share_of_term"),
        F.sum("score").over(w_term).alias("term_sum_score"),
    )
)

with_trends = (
    with_windows
    .select(
        "id",
        "term",
        "enrolled_at",
        "score_share_of_term",
        F.last("enrolled_at", ignorenulls=True).over(w_course_id_term_by_term_sum_s).alias("last_known_enrolled_at"),
        F.round(F.avg("first_score").over(w_course_id_term_by_term_sum_s_2), 2).alias("first_score_avg_3"),
    )
)

result = (
    with_trends
    .select("id", "first_score_avg_3")
    .distinct()
    .orderBy("id")
)
