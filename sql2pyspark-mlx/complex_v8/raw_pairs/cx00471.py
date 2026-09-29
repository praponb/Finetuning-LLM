from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_student_gpa_full = Window.orderBy(F.col("student_gpa").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_by_student_gpa_running = Window.partitionBy("term").orderBy("student_gpa", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status = Window.partitionBy("grade_status")
w_all_by_student_gpa_ties = Window.orderBy(F.col("student_gpa").desc_nulls_first())
w_all_by_student_gpa = Window.orderBy(F.col("student_gpa").desc_nulls_first(), "id")
w_term_by_student_gpa = Window.partitionBy("term").orderBy("student_gpa", "id")

cleaned = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("grade_status") == "PASS"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.upper(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

with_parent_student = (
    cleaned.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.gpa") <= 3.4)
    .select("s.*", F.col("st.gpa").alias("student_gpa"))
)

windowed = (
    with_parent_student
    .select(
        "*",
        F.first("enrolled_at").over(w_all_by_student_gpa_full).alias("first_enrolled_at"),
        F.nth_value("student_gpa", 2).over(w_all_by_student_gpa_full).alias("second_student_gpa"),
        F.first("enrolled_at", ignorenulls=True).over(w_term_by_student_gpa_running).alias("first_known_enrolled_at"),
        F.sum("student_gpa").over(w_grade_status).alias("grade_status_sum_student_gpa"),
        F.round(F.cume_dist().over(w_all_by_student_gpa_ties), 3).alias("student_gpa_cume_dist"),
        (F.col("student_gpa") - F.lag("student_gpa").over(w_all_by_student_gpa)).alias("student_gpa_change"),
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.round(F.col("student_gpa") / F.nullif(F.sum("student_gpa").over(w_grade_status), F.lit(0)), 4).alias("student_gpa_share_of_grade_status"),
        F.dense_rank().over(w_term_by_student_gpa).alias("student_gpa_dense_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "grade_status_clean",
        "student_gpa",
        "first_enrolled_at",
        "first_known_enrolled_at",
        "grade_status_sum_student_gpa",
        "student_gpa_cume_dist",
        "student_gpa_change",
        "rows_in_grade_status",
        "student_gpa_share_of_grade_status",
        "student_gpa_dense_rank",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)
