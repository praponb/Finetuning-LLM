from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())
w_term = Window.partitionBy("term")
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_all_by_enrolled_at_moving_1 = Window.orderBy("enrolled_at", "id").rowsBetween(-1, Window.currentRow)

valid = (
    df_enrollments
    .filter((F.col("grade_status") == "WITHDRAWN") & (F.col("student_id").isNotNull()))
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

deduped = (
    valid
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

unique_rows = (
    deduped
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_details_student = (
    unique_rows.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.is_international")), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "st.student_name",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.major").alias("student_major"),
        "c.course_code",
    )
)

windowed = (
    with_details_student
    .select(
        "*",
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_percent_rank"),
        F.round(F.avg("student_gpa").over(w_term), 2).alias("term_avg_student_gpa"),
        F.sum("student_gpa").over(w_all_by_enrolled_at_moving_1).alias("student_gpa_sum_2"),
    )
)

enriched_course = (
    windowed.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.student_name",
        "s.student_major",
        "s.course_code",
        "s.rows_in_term",
        "s.enrolled_at_percent_rank",
        "s.term_avg_student_gpa",
        F.col("c.dept").alias("course_dept"),
        F.col("c.course_code").alias("course_course_code"),
    )
)

result = (
    enriched_course
    .select(
        "id",
        "term_avg_student_gpa",
        "enrolled_at_percent_rank",
        "score",
    )
    .unpivot(
        ids=["id"],
        values=["term_avg_student_gpa", "enrolled_at_percent_rank", "score"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "measure")
)
