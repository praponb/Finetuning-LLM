from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_course_dept_by_enrolled = Window.partitionBy("term", "course_dept").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_cohort_grade_status = Window.partitionBy("student_cohort", "grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_cohort_grade_status_2 = Window.partitionBy("student_cohort", "grade_status").orderBy("enrolled_at", "id")
w_student_is_international_by = Window.partitionBy("student_is_international").orderBy("enrolled_at", "id")
w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").desc_nulls_first())
w_term_course_dept_by_enrolled_2 = Window.partitionBy("term", "course_dept").orderBy("enrolled_at", "id")
w_all_by_enrolled_at_full = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

enriched = (
    prepared_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .filter(F.upper(F.trim(F.col("st.student_name"))).like("D%"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.is_international").alias("student_is_international"),
        "st.student_name",
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.nth_value("enrolled_at", 2).over(w_term_course_dept_by_enrolled).alias("second_enrolled_at"),
        F.first("enrolled_at").over(w_student_cohort_grade_status).alias("first_enrolled_at"),
        F.row_number().over(w_student_cohort_grade_status_2).alias("enrolled_at_seq"),
        F.dense_rank().over(w_student_is_international_by).alias("enrolled_at_dense_rank"),
        F.dense_rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_dense_rank_2"),
        F.lead("enrolled_at").over(w_student_is_international_by).alias("next_enrolled_at"),
        F.rank().over(w_term_course_dept_by_enrolled_2).alias("enrolled_at_rank"),
        F.ntile(4).over(w_student_is_international_by).alias("enrolled_at_quartile"),
        F.nth_value("enrolled_at", 2).over(w_all_by_enrolled_at_full).alias("second_enrolled_at_2"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_cohort_grade_status_2))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.row_number().over(w_term_course_dept_by_enrolled_2).alias("enrolled_at_seq_2"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_term_course_dept_by_enrolled_2))) / 60, 1).alias("minutes_since_prev_enrolled_at_2"),
        F.ntile(10).over(w_term_course_dept_by_enrolled_2).alias("enrolled_at_decile"),
        F.lag("enrolled_at").over(w_student_is_international_by).alias("prev_enrolled_at"),
    )
)

annotated = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "student_is_international",
        "student_name",
        "course_code",
        "course_dept",
        "second_enrolled_at",
        "enrolled_at_dense_rank",
        "enrolled_at_dense_rank_2",
        "next_enrolled_at",
        "enrolled_at_rank",
        "enrolled_at_quartile",
        "second_enrolled_at_2",
        "minutes_since_prev_enrolled_at",
        "enrolled_at_seq_2",
        "minutes_since_prev_enrolled_at_2",
        "enrolled_at_decile",
        "prev_enrolled_at",
        F.when(F.col("grade_status").isin("WITHDRAWN", "PASS"), "group_a").when(F.col("grade_status") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

result = (
    annotated
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "student_is_international",
        "student_name",
        "course_code",
        "course_dept",
        "enrolled_at_dense_rank",
        "enrolled_at_quartile",
        "second_enrolled_at_2",
        "minutes_since_prev_enrolled_at_2",
        "enrolled_at_decile",
        "grade_status_group",
    )
    .orderBy("course_code", F.col("minutes_since_prev_enrolled_at_2").asc())
)
