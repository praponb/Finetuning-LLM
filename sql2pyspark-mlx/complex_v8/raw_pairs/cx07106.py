from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_student_gpa_running = Window.orderBy(F.col("student_gpa").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_student_major_b = Window.partitionBy("grade_status", "student_major").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_student_id = Window.partitionBy("student_id")
w_grade_status_student_major_b_2 = Window.partitionBy("grade_status", "student_major").orderBy(F.col("score").desc(), "id").rowsBetween(-1, -1)
w_all_by_student_gpa_ties = Window.orderBy(F.col("student_gpa").desc())
w_student_major_student_cohort = Window.partitionBy("student_major", "student_cohort")

cleaned = (
    df_enrollments
    .filter((F.col("grade_status") == "INCOMPLETE") & (F.col("student_id") <= 15))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

with_details_student = (
    cleaned.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_date",
        F.col("st.major").alias("student_major"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

metrics = (
    with_details_student
    .select(
        "*",
        F.sum("student_gpa").over(w_all_by_student_gpa_running).alias("running_student_gpa"),
        F.min("score").over(w_grade_status_student_major_b).alias("min_score_so_far"),
        F.dense_rank().over(w_student_id_by_enrolled_at).alias("enrolled_at_dense_rank"),
        F.size(F.collect_set("term").over(w_student_id)).alias("distinct_term_in_student_id"),
        F.max("score").over(w_grade_status_student_major_b_2).alias("score_max_1_prior"),
        F.rank().over(w_all_by_student_gpa_ties).alias("student_gpa_rank"),
        F.last("score", ignorenulls=True).over(w_grade_status_student_major_b).alias("last_known_score"),
        F.lead("enrolled_at").over(w_student_id_by_enrolled_at).alias("next_enrolled_at"),
        F.min("score").over(w_student_major_student_cohort).alias("student_major_student_cohort_min_score"),
    )
)

first_rows = (
    metrics
    .filter((F.col("student_gpa_rank") <= 3) & (F.col("running_student_gpa") > 7.3))
    .drop("student_gpa_rank")
)

result = (
    first_rows
    .filter(F.col("score_max_1_prior") >= 90.0)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "student_major",
        "student_gpa",
        "running_student_gpa",
        "min_score_so_far",
        "enrolled_at_dense_rank",
        "distinct_term_in_student_id",
        "score_max_1_prior",
        "next_enrolled_at",
    )
    .orderBy(F.col("score_max_1_prior").asc_nulls_last(), "grade_status")
)
