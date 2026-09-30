from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_month = Window.partitionBy(F.month("enrolled_at"))
w_student_id = Window.partitionBy("student_id")
w_all_by_enrolled_at_prior_6 = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-6, -1)
w_course_id_grade_status = Window.partitionBy("course_id", "grade_status")
w_all_by_student_gpa = Window.orderBy("student_gpa", "id")
w_all_by_score = Window.orderBy("score", "id")
w_course_id_enrolled_at_month = Window.partitionBy("course_id", F.date_trunc("month", "enrolled_at")).orderBy(F.col("score").desc())
w_course_id_term_by_enrolled_a = Window.partitionBy("course_id", "term").orderBy(F.col("enrolled_at").desc(), "id")
w_term_student_is_internationa = Window.partitionBy("term", "student_is_international").orderBy("score", "id")
w_all_by_rows_in_student_id_ti = Window.orderBy("rows_in_student_id")

recent = df_enrollments.filter(F.col("term").isin("2024T2", "2024T1", "2025T1"))

combined_details = (
    recent.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

sequenced = (
    combined_details
    .select(
        "*",
        F.sum("score").over(w_enrolled_at_month).alias("enrolled_at_month_sum_score"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.sum("score").over(w_all_by_enrolled_at_prior_6).alias("score_sum_6_prior"),
        F.size(F.collect_set("term").over(w_course_id_grade_status)).alias("distinct_term_in_course_id_grade_status"),
        F.rank().over(w_all_by_student_gpa).alias("student_gpa_rank"),
        F.round(F.percent_rank().over(w_all_by_score), 3).alias("score_percent_rank"),
        F.round(F.percent_rank().over(w_course_id_enrolled_at_month), 3).alias("score_percent_rank_2"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.count(F.lit(1)).over(w_course_id_grade_status).alias("rows_in_course_id_grade_status"),
        F.round(F.cume_dist().over(w_course_id_term_by_enrolled_a), 3).alias("enrolled_at_cume_dist"),
    )
)

top_rows = (
    sequenced
    .filter(F.col("student_gpa_rank") <= 5)
    .drop("student_gpa_rank")
)

with_trends = (
    top_rows
    .select(
        "*",
        F.lag("score").over(w_term_student_is_internationa).alias("prev_score"),
        F.round(F.percent_rank().over(w_all_by_rows_in_student_id_ti), 3).alias("rows_in_student_id_percent_rank"),
        F.lag("enrolled_at_cume_dist", 2, 0).over(w_term_student_is_internationa).alias("prev2_enrolled_at_cume_dist"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "enrolled_at",
        "student_is_international",
        "student_gpa",
        "enrolled_at_month_sum_score",
        "distinct_term_in_course_id_grade_status",
        "score_percent_rank",
        "score_share_of_student_id",
        "enrolled_at_cume_dist",
        "prev_score",
        "rows_in_student_id_percent_rank",
        "prev2_enrolled_at_cume_dist",
    )
    .orderBy(F.col("prev2_enrolled_at_cume_dist").desc_nulls_first(), "id")
    .limit(10)
)
