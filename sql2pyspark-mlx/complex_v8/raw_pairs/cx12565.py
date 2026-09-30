from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_student_id_by_enro = Window.partitionBy("course_id", "student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_term_course_level_by_enrolle = Window.partitionBy("term", "course_level").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_enrolled_on_month_st = Window.partitionBy(F.date_trunc("month", "student_enrolled_on"), "student_cohort")
w_grade_status_group_by_enroll = Window.partitionBy("grade_status_group").orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_grade_status_group_by_enroll_2 = Window.partitionBy("grade_status_group").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_grade_status_group_by_enroll_3 = Window.partitionBy("grade_status_group").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_course_level_by_enrolle_2 = Window.partitionBy("term", "course_level").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_rn_student_cohort_by_first_s = Window.partitionBy("rn", "student_cohort").orderBy("first_score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_group_rn = Window.partitionBy("grade_status_group", "rn")
w_course_id_distinct_term_in_s = Window.partitionBy("course_id", "distinct_term_in_student_enrolled_on_month_stude").orderBy(F.col("last_days_since_enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) | (~F.col("grade_status").isin("FAIL", "PASS")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.when(F.col("grade_status").isin("FAIL", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "PASS", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

with_parent_student = (
    base_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("st.enrolled_on") >= F.lit("2024-05-18").cast("date"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.days_since_enrolled_at",
        "s.grade_status_group",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
        "c.course_code",
        F.col("c.level").alias("course_level"),
    )
)

sequenced = (
    with_parent_student
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status_group",
        "student_enrolled_on",
        "student_cohort",
        "course_code",
        F.row_number().over(w_course_id_student_id_by_enro).alias("rn"),
        F.first("score").over(w_term_course_level_by_enrolle).alias("first_score"),
        F.round(F.col("days_since_enrolled_at") - F.avg("days_since_enrolled_at").over(w_student_enrolled_on_month_st), 2).alias("days_since_enrolled_at_vs_student_enrolled_on_mo"),
        F.row_number().over(w_grade_status_group_by_enroll).alias("enrolled_at_seq"),
        F.ntile(3).over(w_grade_status_group_by_enroll).alias("enrolled_at_tercile"),
        F.round(F.avg("days_since_enrolled_at").over(w_grade_status_group_by_enroll_2), 2).alias("days_since_enrolled_at_avg_4"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_course_id_student_id_by_enro))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.last("days_since_enrolled_at").over(w_grade_status_group_by_enroll_3).alias("last_days_since_enrolled_at"),
        F.first("days_since_enrolled_at", ignorenulls=True).over(w_term_course_level_by_enrolle_2).alias("first_known_days_since_enrolled_at"),
        F.size(F.collect_set("term").over(w_student_enrolled_on_month_st)).alias("distinct_term_in_student_enrolled_on_month_stude"),
    )
)

with_trends = (
    sequenced
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "student_enrolled_on",
        "student_cohort",
        "course_code",
        "rn",
        "first_score",
        "days_since_enrolled_at_vs_student_enrolled_on_mo",
        "enrolled_at_tercile",
        "days_since_enrolled_at_avg_4",
        "minutes_since_prev_enrolled_at",
        "last_days_since_enrolled_at",
        "distinct_term_in_student_enrolled_on_month_stude",
        F.last("student_enrolled_on").over(w_rn_student_cohort_by_first_s).alias("last_student_enrolled_on"),
        F.sum("first_score").over(w_grade_status_group_rn).alias("grade_status_group_rn_sum_first_score"),
        F.nth_value("enrolled_at_tercile", 2).over(w_course_id_distinct_term_in_s).alias("second_enrolled_at_tercile"),
        F.last("days_since_enrolled_at_avg_4").over(w_rn_student_cohort_by_first_s).alias("last_days_since_enrolled_at_avg_4"),
        F.round(F.col("enrolled_at_tercile") / F.nullif(F.sum("enrolled_at_tercile").over(w_grade_status_group_rn), F.lit(0)), 4).alias("enrolled_at_tercile_share_of_grade_status_group_"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "student_enrolled_on",
        "student_cohort",
        "rn",
        "first_score",
        "days_since_enrolled_at_avg_4",
        "minutes_since_prev_enrolled_at",
        "last_days_since_enrolled_at",
        "distinct_term_in_student_enrolled_on_month_stude",
        "last_student_enrolled_on",
        "grade_status_group_rn_sum_first_score",
        "enrolled_at_tercile_share_of_grade_status_group_",
        F.substring(F.trim(F.col("course_code")), 1, 4).alias("course_code_prefix"),
        F.concat(F.col("student_cohort"), F.lit("#"), F.col("enrolled_at_tercile").cast("string")).alias("student_cohort_ref"),
    )
    .orderBy(F.col("rn").desc(), "course_code_prefix", "id")
    .limit(5)
)
