from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("grade_status").orderBy(F.col("student_gpa_2").desc(), F.col("id").desc())

scoped_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

with_details = (
    scoped_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.term_slug",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.major").alias("student_major"),
    )
)

with_parent = (
    with_details.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .filter(F.col("st.gpa") >= 1.7)
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.term_slug",
        "s.student_gpa",
        "s.student_major",
        F.col("st.gpa").alias("student_gpa_2"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

latest_per_key = (
    with_parent
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_details_student = (
    latest_per_key.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term_slug",
        "s.student_gpa",
        "s.student_major",
        "s.student_gpa_2",
        "s.course_credits",
        "s.course_level",
        F.col("st.gpa").alias("student_gpa_3"),
        F.col("st.major").alias("student_major_2"),
    )
)

result = (
    with_details_student
    .select(
        "id",
        "student_gpa_3",
        "student_gpa",
        "student_gpa_2",
    )
    .unpivot(
        ids=["id"],
        values=["student_gpa_3", "student_gpa", "student_gpa_2"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .orderBy("id", "metric")
)
