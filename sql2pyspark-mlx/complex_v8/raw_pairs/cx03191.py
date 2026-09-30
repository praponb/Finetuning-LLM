from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_course_credits = Window.orderBy("course_credits", "id")
w_student_cohort_course_id = Window.partitionBy("student_cohort", "course_id")
w_student_id_term_by_course_cr = Window.partitionBy("student_id", "term").orderBy(F.col("course_credits").desc_nulls_first())
w_all_by_course_credits_full = Window.orderBy("course_credits", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_term_by_course_cr_2 = Window.partitionBy("student_id", "term").orderBy(F.col("course_credits").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_credits = Window.partitionBy("course_credits")
w_term_course_id_by_course_cre = Window.partitionBy("term", "course_id").orderBy("course_credits", "id").rowsBetween(-1, Window.currentRow)
w_term_course_id_by_course_cre_2 = Window.partitionBy("term", "course_id").orderBy("course_credits", "id")
w_student_id_term_by_course_cr_3 = Window.partitionBy("student_id", "term").orderBy(F.col("course_credits").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_course_id_by_course_cre_3 = Window.partitionBy("term", "course_id").orderBy("course_credits")
w_all_by_course_credits_runnin = Window.orderBy(F.col("course_credits").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("grade_status") != "PASS"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

with_details = (
    scoped_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
        F.col("st.major").alias("student_major"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.row_number().over(w_all_by_course_credits).alias("seq_no"),
        F.sum("course_credits").over(w_student_cohort_course_id).alias("student_cohort_course_id_sum_course_credits"),
        F.dense_rank().over(w_student_id_term_by_course_cr).alias("course_credits_dense_rank"),
        F.round(F.avg("course_credits").over(w_student_cohort_course_id), 2).alias("student_cohort_course_id_avg_course_credits"),
        F.first("course_credits").over(w_all_by_course_credits_full).alias("first_course_credits"),
        F.first("course_credits").over(w_student_id_term_by_course_cr_2).alias("first_course_credits_2"),
        F.round(F.col("course_credits") - F.avg("course_credits").over(w_course_credits), 2).alias("course_credits_vs_course_credits_avg"),
        F.round(F.avg("course_credits").over(w_term_course_id_by_course_cre), 2).alias("course_credits_avg_2"),
        F.size(F.collect_set("student_major").over(w_course_credits)).alias("distinct_student_major_in_course_credits"),
        F.size(F.collect_set("term").over(w_student_cohort_course_id)).alias("distinct_term_in_student_cohort_course_id"),
        F.lag("course_credits", 2, 0).over(w_term_course_id_by_course_cre_2).alias("prev2_course_credits"),
        F.sum("course_credits").over(w_student_id_term_by_course_cr_3).alias("running_course_credits"),
        F.round(F.percent_rank().over(w_term_course_id_by_course_cre_3), 3).alias("course_credits_percent_rank"),
        F.round(F.col("course_credits") / F.nullif(F.sum("course_credits").over(w_course_credits), F.lit(0)), 4).alias("course_credits_share_of_course_credits"),
        F.min("course_credits").over(w_all_by_course_credits_runnin).alias("min_course_credits_so_far"),
    )
)

leaders = windowed.filter(F.col("seq_no") == 1)

result = (
    leaders
    .filter(F.col("student_major") == "Biz")
    .select(
        "id",
        "course_id",
        "course_dept",
        "student_major",
        "student_cohort",
        "student_cohort_course_id_avg_course_credits",
        "first_course_credits",
        "first_course_credits_2",
        "course_credits_vs_course_credits_avg",
        "course_credits_avg_2",
        "distinct_student_major_in_course_credits",
        "prev2_course_credits",
        "running_course_credits",
        "course_credits_percent_rank",
        "course_credits_share_of_course_credits",
        "min_course_credits_so_far",
        F.coalesce(F.col("course_dept"), F.lit("UNKNOWN")).alias("course_dept_or_unknown"),
        F.substring(F.trim(F.col("student_major")), 1, 3).alias("student_major_prefix"),
    )
    .orderBy(F.col("distinct_student_major_in_course_credits").desc_nulls_first())
)
