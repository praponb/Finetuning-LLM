from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_student_id_by_enrolled = Window.partitionBy("term", "student_id").orderBy("enrolled_at_date", "id")
w_course_id = Window.partitionBy("course_id")
w_enrolled_at_date_month_cours = Window.partitionBy(F.date_trunc("month", "enrolled_at_date"), "course_credits")
w_all_by_enrolled_at_date_full = Window.orderBy("enrolled_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_course_credits = Window.orderBy(F.col("course_credits").desc_nulls_first(), "id")
w_all_by_course_credits_prior = Window.orderBy("course_credits", "id").rowsBetween(-6, -1)
w_all_by_course_credits_2 = Window.orderBy("course_credits", "id")
w_student_id_course_dept_by_co = Window.partitionBy("student_id", "course_dept").orderBy(F.col("course_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_course_dept_by_co_2 = Window.partitionBy("student_id", "course_dept").orderBy(F.col("course_credits").desc(), "id").rowsBetween(-2, Window.currentRow)
w_course_dept_by_distinct_term = Window.partitionBy("course_dept").orderBy("distinct_term_in_course_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_enrollments
    .filter(F.col("student_id") >= 3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

joined = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .filter(F.col("c.dept") != "Math")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.term_slug",
        "s.enrolled_at_date",
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

ranked = (
    joined
    .select(
        "*",
        F.row_number().over(w_term_student_id_by_enrolled).alias("seq_no"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.min("student_gpa").over(w_enrolled_at_date_month_cours).alias("enrolled_at_date_month_course_credits_min_studen"),
        F.round(F.col("course_credits") - F.avg("course_credits").over(w_course_id), 2).alias("course_credits_vs_course_id_avg"),
        F.size(F.collect_set("term").over(w_enrolled_at_date_month_cours)).alias("distinct_term_in_enrolled_at_date_month_course_c"),
        F.nth_value("student_gpa", 2).over(w_all_by_enrolled_at_date_full).alias("second_student_gpa"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_enrolled_at_date_month_cours), 2).alias("student_gpa_vs_enrolled_at_date_month_course_cre"),
        F.size(F.collect_set("term").over(w_course_id)).alias("distinct_term_in_course_id"),
        F.lag("student_gpa").over(w_term_student_id_by_enrolled).alias("prev_student_gpa"),
        F.dense_rank().over(w_all_by_course_credits).alias("course_credits_dense_rank"),
    )
)

metrics = (
    ranked
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "term_slug",
        "enrolled_at_date",
        "course_credits",
        "rows_in_course_id",
        "enrolled_at_date_month_course_credits_min_studen",
        "course_credits_vs_course_id_avg",
        "distinct_term_in_enrolled_at_date_month_course_c",
        "second_student_gpa",
        "student_gpa_vs_enrolled_at_date_month_course_cre",
        "distinct_term_in_course_id",
        "prev_student_gpa",
        "course_credits_dense_rank",
        F.round(F.avg("seq_no").over(w_all_by_course_credits_prior), 2).alias("seq_no_avg_6_prior"),
        F.row_number().over(w_all_by_course_credits_2).alias("course_credits_seq"),
        F.first("second_student_gpa", ignorenulls=True).over(w_student_id_course_dept_by_co).alias("first_known_second_student_gpa"),
        F.sum("second_student_gpa").over(w_student_id_course_dept_by_co_2).alias("second_student_gpa_sum_3"),
        F.sum("student_gpa").over(w_course_dept_by_distinct_term).alias("running_student_gpa"),
        F.lead("distinct_term_in_course_id", 1, 0).over(w_all_by_course_credits_2).alias("next_distinct_term_in_course_id"),
    )
)

result = (
    metrics
    .filter(F.col("course_id") <= 6)
    .select(
        "id",
        "term_slug",
        "enrolled_at_date",
        "course_credits",
        "rows_in_course_id",
        "enrolled_at_date_month_course_credits_min_studen",
        "course_credits_vs_course_id_avg",
        "second_student_gpa",
        "student_gpa_vs_enrolled_at_date_month_course_cre",
        "distinct_term_in_course_id",
        "course_credits_dense_rank",
        "course_credits_seq",
        "first_known_second_student_gpa",
        "second_student_gpa_sum_3",
        "running_student_gpa",
    )
    .orderBy("course_credits")
)
