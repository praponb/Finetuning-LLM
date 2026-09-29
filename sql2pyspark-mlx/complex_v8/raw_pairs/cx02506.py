from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status")
w_student_is_international_2 = Window.partitionBy("student_is_international_2")
w_grade_status_student_is_inte = Window.partitionBy("grade_status", "student_is_international").orderBy("student_gpa", "id").rowsBetween(-2, Window.currentRow)
w_grade_status_student_is_inte_2 = Window.partitionBy("grade_status", "student_is_international").orderBy("student_gpa", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_enrollments
    .filter((F.col("student_id") <= 15) | (F.col("term").isNotNull()))
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

with_trends = (
    recent
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.max("score").over(w_grade_status).alias("grade_status_max_score"),
        F.size(F.collect_set("course_id").over(w_grade_status)).alias("distinct_course_id_in_grade_status"),
    )
)

joined_student = (
    with_trends.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.gpa").between(2.7, 3.4)), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.term",
        "s.grade_status",
        "s.grade_status_max_score",
        "s.distinct_course_id_in_grade_status",
        F.col("st.is_international").alias("student_is_international"),
    )
)

joined_student_2 = (
    joined_student.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.major").isNotNull()), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.term",
        "s.grade_status",
        "s.distinct_course_id_in_grade_status",
        "s.student_is_international",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.major").alias("student_major"),
        F.col("st.is_international").alias("student_is_international_2"),
    )
)

with_trends_2 = (
    joined_student_2
    .select(
        "*",
        F.count(F.lit(1)).over(w_student_is_international_2).alias("rows_in_student_is_international_2"),
        F.round(F.avg("student_gpa").over(w_grade_status_student_is_inte), 2).alias("student_gpa_avg_3"),
        F.nth_value("student_gpa", 2).over(w_grade_status_student_is_inte_2).alias("second_student_gpa"),
        F.size(F.collect_set("term").over(w_student_is_international_2)).alias("distinct_term_in_student_is_international_2"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_student_is_international_2), 2).alias("student_gpa_vs_student_is_international_2_avg"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "distinct_course_id_in_grade_status",
        "student_gpa",
        "student_major",
        "rows_in_student_is_international_2",
        "student_gpa_avg_3",
        "second_student_gpa",
        "distinct_term_in_student_is_international_2",
        "student_gpa_vs_student_is_international_2_avg",
    )
    .orderBy(
        "term",
        F.col("distinct_term_in_student_is_international_2").desc_nulls_first(),
    )
)
