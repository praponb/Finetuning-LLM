from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_dept = Window.partitionBy("course_dept")
w_all_by_enrolled_at_moving_6 = Window.orderBy("enrolled_at", "id").rowsBetween(-6, Window.currentRow)
w_course_id_grade_status_group = Window.partitionBy("course_id", "grade_status_group")
w_course_dept_by_enrolled_at = Window.partitionBy("course_dept").orderBy("enrolled_at", "id")
w_grade_status_group_by_score = Window.partitionBy("grade_status_group").orderBy("score", "id")
w_term_student_cohort_by_stude = Window.partitionBy("term", "student_cohort").orderBy(F.col("student_gpa").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_dept_by_enrolled_at_f = Window.partitionBy("course_dept").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_moving_1 = Window.orderBy("enrolled_at", "id").rowsBetween(-1, Window.currentRow)
w_course_dept_by_enrolled_at_t = Window.partitionBy("course_dept").orderBy("enrolled_at")
w_term_student_cohort_by_stude_2 = Window.partitionBy("term", "student_cohort").orderBy(F.col("student_gpa").asc_nulls_last())

recent = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-13 11:00:00").cast("timestamp")) | (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("PASS", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

with_parent_student = (
    recent.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_group",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("c.dept").alias("course_dept"),
    )
)

with_windows = (
    with_parent_student
    .select(
        "*",
        F.min("score").over(w_course_dept).alias("course_dept_min_score"),
        F.round(F.avg("student_gpa").over(w_all_by_enrolled_at_moving_6), 2).alias("student_gpa_avg_7"),
        F.size(F.collect_set("grade_status").over(w_course_id_grade_status_group)).alias("distinct_grade_status_in_course_id_grade_status_"),
        F.dense_rank().over(w_course_dept_by_enrolled_at).alias("enrolled_at_dense_rank"),
        F.sum("student_gpa").over(w_course_id_grade_status_group).alias("course_id_grade_status_group_sum_student_gpa"),
        F.row_number().over(w_grade_status_group_by_score).alias("score_seq"),
        F.max("student_gpa").over(w_course_id_grade_status_group).alias("course_id_grade_status_group_max_student_gpa"),
        F.count(F.lit(1)).over(w_term_student_cohort_by_stude).alias("running_count"),
        F.max("student_gpa").over(w_course_dept).alias("course_dept_max_student_gpa"),
        F.round(F.avg("score").over(w_course_dept), 2).alias("course_dept_avg_score"),
        F.last("student_gpa").over(w_course_dept_by_enrolled_at_f).alias("last_student_gpa"),
        F.sum("student_gpa").over(w_all_by_enrolled_at_moving_1).alias("student_gpa_sum_2"),
        F.size(F.collect_set("grade_status_group").over(w_course_dept)).alias("distinct_grade_status_group_in_course_dept"),
        F.round(F.cume_dist().over(w_course_dept_by_enrolled_at_t), 3).alias("enrolled_at_cume_dist"),
        F.rank().over(w_term_student_cohort_by_stude_2).alias("student_gpa_rank"),
    )
)

first_rows = (
    with_windows
    .filter(F.col("score_seq") == 1)
    .drop("score_seq")
)

result = (
    first_rows
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "student_gpa",
        "course_dept",
        "course_dept_min_score",
        "student_gpa_avg_7",
        "distinct_grade_status_in_course_id_grade_status_",
        "enrolled_at_dense_rank",
        "course_id_grade_status_group_sum_student_gpa",
        "course_id_grade_status_group_max_student_gpa",
        "running_count",
        "course_dept_max_student_gpa",
        "student_gpa_sum_2",
        "distinct_grade_status_group_in_course_dept",
        "enrolled_at_cume_dist",
        "student_gpa_rank",
        (F.col("enrolled_at") + F.expr("INTERVAL 1 DAYS")).alias("enrolled_at_plus_1d"),
        F.length(F.trim(F.col("course_dept"))).alias("course_dept_len"),
    )
    .orderBy(F.col("course_dept_len").asc(), F.col("id").asc())
)
