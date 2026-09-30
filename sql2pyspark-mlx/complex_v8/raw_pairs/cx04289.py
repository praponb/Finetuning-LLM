from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("student_id").orderBy(F.col("days_since_enrolled_at").desc(), F.col("id").desc())

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.abs(F.col("score") - 56.1).alias("score_distance"),
    )
)

latest_per_key = (
    cleaned_enrollments
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

derived = (
    latest_per_key
    .filter(F.col("term").isNotNull())
    .select(
        "*",
        (F.floor(F.col("days_since_enrolled_at") / 100) * 100).alias("days_since_enrolled_at_bucket"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
        F.length(F.trim(F.col("term_prefix"))).alias("term_prefix_len"),
    )
)

with_details_student = (
    derived.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        "s.term_prefix",
        "s.days_since_enrolled_at",
        "s.score_distance",
        "s.days_since_enrolled_at_bucket",
        "s.enrolled_at_label",
        "s.term_prefix_len",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.major").alias("student_major"),
        "st.student_name",
        "c.course_code",
    )
)

result = (
    with_details_student
    .rollup("student_id", "student_major", "course_id")
    .agg(
        F.sum(F.when(F.upper(F.trim(F.col("student_name"))).like("J%"), F.col("days_since_enrolled_at_bucket"))).alias("days_since_enrolled_at_bucket_upper_trim_student"),
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("student_major").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
