from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_student_id_by_student_enroll = Window.partitionBy("student_id").orderBy("student_enrolled_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id = Window.partitionBy("course_id")
w_student_id_by_student_enroll_2 = Window.partitionBy("student_id").orderBy("student_enrolled_on", "id")
w_student_id_by_score_full = Window.partitionBy("student_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_by_student_enroll_3 = Window.partitionBy("student_id").orderBy("student_enrolled_on")
w_student_id_by_score = Window.partitionBy("student_id").orderBy("score", "id")
w_student_id_by_student_enroll_4 = Window.partitionBy("student_id").orderBy("student_enrolled_on", "id").rowsBetween(-3, -1)
w_student_id_by_score_running = Window.partitionBy("student_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_prev2_student_enrolled_on_ye = Window.partitionBy(F.year("prev2_student_enrolled_on"), "student_id")
w_next_student_enrolled_on_mon = Window.partitionBy(F.month("next_student_enrolled_on")).orderBy("rows_in_course_id", "id").rowsBetween(-2, -1)

recent = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-24 08:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

enriched_course = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "c.course_code",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

ranked = (
    enriched_course
    .select(
        "*",
        F.row_number().over(w_all_by_score).alias("seq_no"),
        F.last("score", ignorenulls=True).over(w_student_id_by_student_enroll).alias("last_known_score"),
        F.min("score").over(w_course_id).alias("course_id_min_score"),
        F.lag("score").over(w_student_id_by_student_enroll_2).alias("prev_score"),
        F.round(F.avg("score").over(w_course_id), 2).alias("course_id_avg_score"),
        F.lag("student_enrolled_on", 2).over(w_all_by_score).alias("prev2_student_enrolled_on"),
        F.last("student_enrolled_on").over(w_student_id_by_score_full).alias("last_student_enrolled_on"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.lead("student_enrolled_on").over(w_all_by_score).alias("next_student_enrolled_on"),
        F.dense_rank().over(w_student_id_by_student_enroll_3).alias("student_enrolled_on_dense_rank"),
        F.round(F.percent_rank().over(w_student_id_by_score), 3).alias("score_percent_rank"),
        F.sum("score").over(w_student_id_by_student_enroll_4).alias("score_sum_3_prior"),
        F.count(F.lit(1)).over(w_student_id_by_score_running).alias("running_count"),
    )
)

top_rows = (
    ranked
    .filter(F.col("student_enrolled_on_dense_rank") == 1)
    .drop("student_enrolled_on_dense_rank")
)

ranked_2 = (
    top_rows
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "course_code",
        "student_enrolled_on",
        "seq_no",
        "last_known_score",
        "course_id_avg_score",
        "prev2_student_enrolled_on",
        "last_student_enrolled_on",
        "rows_in_course_id",
        "next_student_enrolled_on",
        "score_percent_rank",
        "score_sum_3_prior",
        "running_count",
        F.size(F.collect_set("grade_status").over(w_prev2_student_enrolled_on_ye)).alias("distinct_grade_status_in_prev2_student_enrolled_"),
        F.count(F.lit(1)).over(w_prev2_student_enrolled_on_ye).alias("rows_in_prev2_student_enrolled_on_year_student_i"),
        F.max("score").over(w_next_student_enrolled_on_mon).alias("score_max_2_prior"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "course_code",
        "student_enrolled_on",
        "seq_no",
        "last_known_score",
        "course_id_avg_score",
        "prev2_student_enrolled_on",
        "last_student_enrolled_on",
        "rows_in_course_id",
        "next_student_enrolled_on",
        "score_sum_3_prior",
        "distinct_grade_status_in_prev2_student_enrolled_",
        "rows_in_prev2_student_enrolled_on_year_student_i",
        "score_max_2_prior",
    )
    .orderBy(F.col("grade_status").desc())
)
