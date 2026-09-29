from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_running = Window.orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score_ties = Window.orderBy(F.col("score").desc())
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_course_id_grade_status_by_pr = Window.partitionBy("course_id", "grade_status").orderBy(F.col("prev_score").desc_nulls_first())
w_all_by_prev_score = Window.orderBy("prev_score", "id")
w_all_by_prev_score_ties = Window.orderBy("prev_score")
w_course_credits = Window.partitionBy("course_credits")
w_course_id_grade_status_by_pr_2 = Window.partitionBy("course_id", "grade_status").orderBy(F.col("prev_score").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_enrollments
    .filter(F.col("term").isin("2025T2", "2025T1", "2024T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.max("score").over(w_all_by_score_running).alias("max_score_so_far"),
        F.rank().over(w_all_by_score_ties).alias("score_rank"),
        F.lag("score").over(w_all_by_score).alias("prev_score"),
        F.round(F.avg("score").over(w_grade_status_course_id), 2).alias("grade_status_course_id_avg_score"),
        (F.col("score") - F.lag("score").over(w_all_by_score)).alias("score_change"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status_course_id), F.lit(0)), 4).alias("score_share_of_grade_status_course_id"),
    )
)

with_details = (
    with_windows.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.enrolled_on").between(F.lit("2024-02-28").cast("date"), F.lit("2025-07-18").cast("date"))), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.prev_score",
        "s.grade_status_course_id_avg_score",
        "s.score_share_of_grade_status_course_id",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.major").alias("student_major"),
    )
)

enriched_course = (
    with_details.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.prev_score",
        "s.score_share_of_grade_status_course_id",
        F.col("c.credits").alias("course_credits"),
    )
)

with_trends = (
    enriched_course
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "score_share_of_grade_status_course_id",
        "course_credits",
        F.round(F.percent_rank().over(w_course_id_grade_status_by_pr), 3).alias("prev_score_percent_rank"),
        F.round(F.cume_dist().over(w_course_id_grade_status_by_pr), 3).alias("prev_score_cume_dist"),
        F.lag("score", 3).over(w_all_by_prev_score).alias("prev3_score"),
        F.round(F.percent_rank().over(w_all_by_prev_score_ties), 3).alias("prev_score_percent_rank_2"),
        F.size(F.collect_set("grade_status").over(w_course_credits)).alias("distinct_grade_status_in_course_credits"),
        F.first("prev_score").over(w_course_id_grade_status_by_pr_2).alias("first_prev_score"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "score_share_of_grade_status_course_id",
        "course_credits",
        "prev_score_percent_rank",
        "prev_score_cume_dist",
        "prev3_score",
        "prev_score_percent_rank_2",
        "distinct_grade_status_in_course_credits",
        "first_prev_score",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.least(F.col("prev3_score"), F.col("course_credits")).alias("least_prev3_score_course_credits"),
    )
    .orderBy(F.col("course_id").asc_nulls_last(), "term")
)
