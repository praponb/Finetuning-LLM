from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_month_grade_stat = Window.partitionBy("enrolled_at_month", "grade_status").orderBy(F.col("enrolled_at_month").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_group = Window.partitionBy("grade_status_group")

valid_enrollments = (
    df_enrollments
    .filter((F.col("score") < 62.5) & (F.col("grade_status") == "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
        F.when(F.col("grade_status").isin("PASS", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
    )
)

joined_student = (
    valid_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.col("st.gpa").alias("student_gpa"),
        "st.student_name",
        F.col("st.cohort").alias("student_cohort"),
    )
)

ranked = (
    joined_student
    .select(
        "*",
        F.last("enrolled_at_month", ignorenulls=True).over(w_enrolled_at_month_grade_stat).alias("last_known_enrolled_at_month"),
        F.sum("student_gpa").over(w_grade_status_group).alias("grade_status_group_sum_student_gpa"),
        F.first("enrolled_at", ignorenulls=True).over(w_enrolled_at_month_grade_stat).alias("first_known_enrolled_at"),
        F.sum("enrolled_at_month").over(w_grade_status_group).alias("grade_status_group_sum_enrolled_at_month"),
        F.last("score", ignorenulls=True).over(w_enrolled_at_month_grade_stat).alias("last_known_score"),
    )
)

screened = (
    ranked.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.term") == "2024T2"), "left_anti")
)

result = (
    screened
    .filter(F.month(F.col("first_known_enrolled_at")).isin(1, 3))
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "term_ref",
        "grade_status_group",
        "enrolled_at_month",
        "student_gpa",
        "student_name",
        "last_known_enrolled_at_month",
        "grade_status_group_sum_student_gpa",
        "first_known_enrolled_at",
        "grade_status_group_sum_enrolled_at_month",
        "last_known_score",
        F.substring(F.trim(F.col("term")), 1, 4).alias("term_prefix"),
    )
    .orderBy("term", "id")
)
