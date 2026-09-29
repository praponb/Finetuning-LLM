from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_rows_by_median_course_cred = Window.partitionBy("n_rows").orderBy(F.col("median_course_credits").desc(), "course_credits", "grade_status_group")
w_n_rows_by_median_course_cred_2 = Window.partitionBy("n_rows").orderBy(F.col("median_course_credits").desc(), "course_credits", "grade_status_group").rowsBetween(-3, -1)
w_distinct_ids_grade_status_gr = Window.partitionBy("distinct_ids", "grade_status_group")
w_distinct_ids_grade_status_gr_2 = Window.partitionBy("distinct_ids", "grade_status_group").orderBy(F.col("median_course_credits").asc_nulls_last(), "course_credits", "grade_status_group").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2024T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("PASS", "INCOMPLETE"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

line_level = (
    base_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term") == "2024T2"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status_group",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.term").alias("enrollment_term"),
    )
)

enriched_course = (
    line_level.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.enrollment_student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status_group",
        "s.enrollment_id",
        "s.enrollment_enrolled_at",
        "s.enrollment_grade_status",
        "s.enrollment_student_id",
        "s.enrollment_term",
        F.col("c.credits").alias("course_credits"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

stats = (
    enriched_course
    .filter(F.col("enrollment_term") == "2024T2")
    .groupBy("course_credits", "grade_status_group")
    .agg(
        F.sum("course_credits").alias("total_course_credits"),
        F.array_sort(F.collect_set("student_cohort")).alias("student_cohort_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("course_credits", 0.5).alias("median_course_credits"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

ranked = (
    stats
    .select(
        "*",
        F.lead("distinct_ids").over(w_n_rows_by_median_course_cred).alias("next_distinct_ids"),
        F.sum("n_rows").over(w_n_rows_by_median_course_cred_2).alias("n_rows_sum_3_prior"),
        F.min("total_course_credits").over(w_distinct_ids_grade_status_gr).alias("distinct_ids_grade_status_group_min_total_course"),
        F.round(F.col("course_credits") - F.avg("course_credits").over(w_distinct_ids_grade_status_gr), 2).alias("course_credits_vs_distinct_ids_grade_status_grou"),
        F.count(F.lit(1)).over(w_distinct_ids_grade_status_gr).alias("rows_in_distinct_ids_grade_status_group"),
        F.nth_value("total_course_credits", 2).over(w_distinct_ids_grade_status_gr_2).alias("second_total_course_credits"),
    )
)

result = (
    ranked
    .filter(F.col("median_course_credits") >= 2)
    .select(
        "course_credits",
        "total_course_credits",
        "student_cohort_list",
        "distinct_ids",
        "median_course_credits",
        "n_rows",
        "next_distinct_ids",
        "n_rows_sum_3_prior",
        "distinct_ids_grade_status_group_min_total_course",
        "course_credits_vs_distinct_ids_grade_status_grou",
        "rows_in_distinct_ids_grade_status_group",
        "second_total_course_credits",
    )
)
