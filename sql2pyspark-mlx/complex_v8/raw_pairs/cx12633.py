from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_enrolled_at_ru = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_dept = Window.partitionBy("course_dept")
w_course_dept_by_course_credit = Window.partitionBy("course_dept").orderBy(F.col("course_credits").desc(), "id").rowsBetween(-3, -1)
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_course_dept_by_course_credit_2 = Window.partitionBy("course_dept").orderBy(F.col("course_credits").desc(), "id")
w_course_id_student_cohort_by = Window.partitionBy("course_id", "student_cohort").orderBy(F.col("student_enrolled_on").asc_nulls_last(), "id")
w_student_enrolled_on_month_co = Window.partitionBy(F.date_trunc("month", "student_enrolled_on"), "course_id")
w_course_dept_by_course_credit_3 = Window.partitionBy("course_dept").orderBy(F.col("course_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-24 13:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

with_parent_course = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_clean",
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

ranked = (
    with_parent_course
    .select(
        "*",
        F.sum("course_credits").over(w_student_id_by_enrolled_at_ru).alias("running_course_credits"),
        F.round(F.col("course_credits") - F.avg("course_credits").over(w_course_dept), 2).alias("course_credits_vs_course_dept_avg"),
        F.sum("course_credits").over(w_course_dept_by_course_credit).alias("course_credits_sum_3_prior"),
        F.count(F.lit(1)).over(w_course_dept).alias("rows_in_course_dept"),
        F.datediff(F.col("student_enrolled_on"), F.lag("student_enrolled_on").over(w_student_id_by_enrolled_at)).alias("days_since_prev_student_enrolled_on"),
        (F.col("course_credits") - F.lag("course_credits").over(w_course_dept_by_course_credit_2)).alias("course_credits_change"),
        F.round(F.percent_rank().over(w_course_id_student_cohort_by), 3).alias("student_enrolled_on_percent_rank"),
        F.row_number().over(w_course_dept_by_course_credit_2).alias("rn"),
        F.round(F.avg("course_credits").over(w_student_enrolled_on_month_co), 2).alias("student_enrolled_on_month_course_id_avg_course_c"),
        F.max("course_credits").over(w_course_dept_by_course_credit_3).alias("max_course_credits_so_far"),
        F.round(F.col("course_credits") / F.nullif(F.sum("course_credits").over(w_course_dept), F.lit(0)), 4).alias("course_credits_share_of_course_dept"),
        F.min("course_credits").over(w_course_dept).alias("course_dept_min_course_credits"),
        F.size(F.collect_set("grade_status").over(w_course_dept)).alias("distinct_grade_status_in_course_dept"),
        F.lead("course_credits", 3).over(w_course_id_student_cohort_by).alias("next3_course_credits"),
    )
)

derived = (
    ranked
    .filter(F.col("days_since_prev_student_enrolled_on") >= 30)
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "course_level",
        "course_credits",
        "student_enrolled_on",
        "student_cohort",
        "running_course_credits",
        "course_credits_vs_course_dept_avg",
        "course_credits_sum_3_prior",
        "days_since_prev_student_enrolled_on",
        "course_credits_change",
        "student_enrolled_on_percent_rank",
        "rn",
        "student_enrolled_on_month_course_id_avg_course_c",
        "max_course_credits_so_far",
        "course_credits_share_of_course_dept",
        "distinct_grade_status_in_course_dept",
        "next3_course_credits",
        F.abs(F.col("running_course_credits") - 19).alias("running_course_credits_distance"),
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
        F.date_format(F.col("student_enrolled_on"), "yyyy-MM").alias("student_enrolled_on_label"),
    )
)

result = (
    derived
    .filter(F.col("course_credits_vs_course_dept_avg") >= -2.0)
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "course_credits",
        "running_course_credits",
        "course_credits_vs_course_dept_avg",
        "course_credits_sum_3_prior",
        "days_since_prev_student_enrolled_on",
        "course_credits_change",
        "student_enrolled_on_percent_rank",
        "rn",
        "student_enrolled_on_month_course_id_avg_course_c",
        "max_course_credits_so_far",
        "course_credits_share_of_course_dept",
        "distinct_grade_status_in_course_dept",
        "next3_course_credits",
        "running_course_credits_distance",
        (F.col("running_course_credits") + F.col("course_credits_change")).alias("running_course_credits_plus_course_credits_chang"),
    )
    .orderBy("next3_course_credits", F.col("course_id").desc())
)
