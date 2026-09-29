from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_enrolled_at_month_course_id = Window.partitionBy(F.month("enrolled_at"), "course_id")
w_all_by_student_enrolled_on = Window.orderBy(F.col("student_enrolled_on").asc_nulls_last(), "id")
w_term_by_score = Window.partitionBy("term").orderBy("score", "id")
w_student_cohort_course_id_by = Window.partitionBy("student_cohort", "course_id").orderBy("score", "id")
w_term_by_score_running = Window.partitionBy("term").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_by_score_moving_1 = Window.partitionBy("term").orderBy("score", "id").rowsBetween(-1, Window.currentRow)
w_course_id_term = Window.partitionBy("course_id", "term")

cleaned = (
    df_enrollments
    .filter(F.col("grade_status").isin("PASS", "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

with_parent_student = (
    cleaned.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

metrics = (
    with_parent_student
    .select(
        "*",
        F.row_number().over(w_all_by_score).alias("seq_no"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_month_course_id), F.lit(0)), 4).alias("score_share_of_enrolled_at_month_course_id"),
        F.min("score").over(w_enrolled_at_month_course_id).alias("enrolled_at_month_course_id_min_score"),
        F.round(F.cume_dist().over(w_all_by_student_enrolled_on), 3).alias("student_enrolled_on_cume_dist"),
        F.lag("score").over(w_term_by_score).alias("prev_score"),
        F.round(F.col("score") - F.avg("score").over(w_enrolled_at_month_course_id), 2).alias("score_vs_enrolled_at_month_course_id_avg"),
        F.count(F.lit(1)).over(w_enrolled_at_month_course_id).alias("rows_in_enrolled_at_month_course_id"),
        F.round(F.cume_dist().over(w_student_cohort_course_id_by), 3).alias("score_cume_dist"),
        F.first("enrolled_at", ignorenulls=True).over(w_term_by_score_running).alias("first_known_enrolled_at"),
        F.max("score").over(w_term_by_score_moving_1).alias("score_max_2"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id_term), F.lit(0)), 4).alias("score_share_of_course_id_term"),
    )
)

result = (
    metrics
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "student_enrolled_on",
        "seq_no",
        "score_share_of_enrolled_at_month_course_id",
        "student_enrolled_on_cume_dist",
        "prev_score",
        "score_vs_enrolled_at_month_course_id_avg",
        "rows_in_enrolled_at_month_course_id",
        "score_cume_dist",
        "score_max_2",
        "score_share_of_course_id_term",
        (F.col("prev_score") - F.col("score_cume_dist")).alias("prev_score_minus_score_cume_dist"),
        F.when(F.col("student_cohort").isin("C2023", "C2022"), "group_a").when(F.col("student_cohort") == "C2021", "group_b").otherwise("other").alias("student_cohort_group"),
    )
)
