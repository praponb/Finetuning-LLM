from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("student_id").orderBy(F.col("prior_rows_1d").desc(), F.col("id").desc())

in_window = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.score").alias("prior_score_1d"),
    )
)

unique_rows = (
    in_window
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_parent_student = (
    unique_rows.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.major").alias("student_major"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

screened = (
    with_parent_student.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.cohort") == "C2021") & (F.col("st.gpa") <= 3.2), "left_semi")
)

result = (
    screened
    .groupBy("enrolled_at", "student_id")
    .pivot("student_major", ["Bio", "Biz", "Math", "Art"])
    .agg(F.min("student_gpa"))
    .orderBy("enrolled_at", "student_id")
)
