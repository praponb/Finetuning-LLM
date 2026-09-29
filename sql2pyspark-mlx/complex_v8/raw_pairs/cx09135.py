from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("student_cohort").orderBy(F.col("course_credits").desc(), F.col("id").desc())

scoped = (
    df_enrollments
    .filter(F.col("score") < 59.7)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

joined = (
    scoped.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .filter(F.col("c.credits") < 4)
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

unique_rows = (
    joined
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_parent = (
    unique_rows.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.course_code",
        "s.course_credits",
        "s.student_cohort",
        "s.student_gpa",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
        F.col("st.gpa").alias("student_gpa_2"),
    )
)

result = (
    with_parent
    .filter(F.col("course_dept").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "course_code",
        "student_cohort",
        "student_gpa",
        "course_dept",
        "student_gpa_2",
    )
)
