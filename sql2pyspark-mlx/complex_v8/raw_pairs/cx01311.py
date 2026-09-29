from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_enrollment_enrolled_at = Window.partitionBy(F.year("first_enrollment_enrolled_at")).orderBy(F.col("enrollment_grade_status_count").desc())
w_first_enrollment_enrolled_at_2 = Window.partitionBy(F.year("first_enrollment_enrolled_at")).orderBy(F.col("enrollment_grade_status_count").desc(), "id")
w_enrolled_at_month_student_id = Window.partitionBy(F.month("enrolled_at"), "student_id")
w_first_enrollment_enrolled_at_3 = Window.partitionBy(F.year("first_enrollment_enrolled_at")).orderBy(F.col("enrollment_grade_status_count").desc(), "id").rowsBetween(-3, Window.currentRow)
w_n_grade_status_not_in_incomp = Window.partitionBy("n_grade_status_not_in_incomple", "enrollment_grade_status_count_seq").orderBy("max_enrollment_grade_status_count", "grade_status", "enrollment_grade_status_count_seq")
w_all_by_n_grade_status_not_in = Window.orderBy(F.col("n_grade_status_not_in_incomple").desc(), "grade_status", "enrollment_grade_status_count_seq").rowsBetween(-2, Window.currentRow)
w_all_by_n_grade_status_not_in_2 = Window.orderBy(F.col("n_grade_status_not_in_incomple").desc(), "grade_status", "enrollment_grade_status_count_seq")
w_n_grade_status_not_in_incomp_2 = Window.partitionBy("n_grade_status_not_in_incomple", "enrollment_grade_status_count_seq").orderBy("max_enrollment_grade_status_count")
w_enrollment_grade_status_coun = Window.partitionBy("enrollment_grade_status_count_seq", "grade_status")

scoped = (
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
        F.abs(F.col("score") - 57.6).alias("score_distance"),
        F.substring(F.trim(F.col("term")), 1, 3).alias("term_prefix"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("grade_status") != "INCOMPLETE", 1)).alias("enrollment_grade_status_count"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

summarized_rows = (
    scoped.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        "e.first_enrollment_enrolled_at",
    )
)

windowed = (
    summarized_rows
    .select(
        "*",
        F.rank().over(w_first_enrollment_enrolled_at).alias("enrollment_grade_status_count_rank"),
        F.row_number().over(w_first_enrollment_enrolled_at_2).alias("enrollment_grade_status_count_seq"),
        F.count(F.lit(1)).over(w_enrolled_at_month_student_id).alias("rows_in_enrolled_at_month_student_id"),
        F.sum("score").over(w_first_enrollment_enrolled_at_3).alias("score_sum_4"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_month_student_id), F.lit(0)), 4).alias("score_share_of_enrolled_at_month_student_id"),
    )
)

enriched_course = (
    windowed.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.level") == "intro"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.score_distance",
        "s.term_prefix",
        "s.enrollment_grade_status_count",
        "s.enrollment_grade_status_count_rank",
        "s.enrollment_grade_status_count_seq",
        "s.rows_in_enrolled_at_month_student_id",
        "s.score_sum_4",
        "s.score_share_of_enrolled_at_month_student_id",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
        F.col("st.major").alias("student_major"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

summary = (
    enriched_course
    .groupBy("grade_status", "enrollment_grade_status_count_seq")
    .agg(
        F.max("enrollment_grade_status_count").alias("max_enrollment_grade_status_count"),
        F.count(F.when(~F.col("grade_status").isin("INCOMPLETE", "FAIL"), 1)).alias("n_grade_status_not_in_incomple"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.array_sort(F.collect_set("course_dept")).alias("course_dept_list"),
        F.count("*").alias("row_count"),
    )
)

windowed_2 = (
    summary
    .select(
        "grade_status",
        "enrollment_grade_status_count_seq",
        "max_enrollment_grade_status_count",
        "grade_status_list",
        "course_dept_list",
        "row_count",
        F.round(F.cume_dist().over(w_n_grade_status_not_in_incomp), 3).alias("max_enrollment_grade_status_count_cume_dist"),
        F.max("row_count").over(w_all_by_n_grade_status_not_in).alias("row_count_max_3"),
        (F.col("row_count") - F.lag("row_count").over(w_all_by_n_grade_status_not_in_2)).alias("row_count_change"),
        F.dense_rank().over(w_n_grade_status_not_in_incomp_2).alias("max_enrollment_grade_status_count_dense_rank"),
        F.sum("row_count").over(w_enrollment_grade_status_coun).alias("enrollment_grade_status_count_seq_grade_status_s"),
    )
)

first_rows = windowed_2.filter(F.col("max_enrollment_grade_status_count_dense_rank") == 1)

result = (
    first_rows
    .groupBy("max_enrollment_grade_status_count")
    .pivot("grade_status", ["WITHDRAWN", "FAIL", "PASS"])
    .agg(F.sum("enrollment_grade_status_count_seq_grade_status_s").alias("amount"), F.count("enrollment_grade_status_count_seq_grade_status_s").alias("n"))
)
