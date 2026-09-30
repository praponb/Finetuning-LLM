from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_student_major_by = Window.partitionBy("student_id", "student_major").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id = Window.partitionBy("student_id")

filtered = (
    df_enrollments
    .filter(F.col("score") < 83.1)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

enriched_student = (
    filtered.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        F.col("st.is_international").alias("student_is_international"),
        "st.student_name",
        F.col("st.major").alias("student_major"),
        F.col("c.credits").alias("course_credits"),
    )
)

metrics = (
    enriched_student
    .select(
        "*",
        F.first("course_credits").over(w_student_id_student_major_by).alias("first_course_credits"),
        F.round(F.avg("course_credits").over(w_student_id), 2).alias("student_id_avg_course_credits"),
    )
)

result = (
    metrics
    .select("id", "first_course_credits", "course_credits")
    .unpivot(
        ids=["id"],
        values=["first_course_credits", "course_credits"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
