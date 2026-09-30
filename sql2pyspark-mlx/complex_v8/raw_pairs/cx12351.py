from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("student_id").orderBy(F.col("enrollment_score").desc(), F.col("id").desc(), F.col("enrollment_id").desc())

cleaned = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
        F.substring(F.trim(F.col("grade_status")), 1, 4).alias("grade_status_prefix"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

tagged = (
    cleaned
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "enrolled_at_week",
        F.when(F.col("grade_status").isin("INCOMPLETE", "FAIL"), "group_a").when(F.col("grade_status") == "PASS", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

detail_rows_enrollments = (
    tagged.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at_week",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
    )
)

latest_per_key = (
    detail_rows_enrollments
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    latest_per_key.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.id") == F.col("s.enrollment_id")) & (F.col("e.enrolled_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")), "inner")
    .filter(F.col("e.grade_status") != "FAIL")
    .select("s.*", F.col("e.grade_status").alias("enrollment_grade_status"))
)

without_matches = (
    joined.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.lower(F.col("st.student_name")).like("%ch%")), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "enrolled_at_week",
        "enrollment_id",
        "enrollment_score",
        "enrollment_enrolled_at",
        "enrollment_term",
        "enrollment_grade_status",
        F.round((F.unix_timestamp(F.col("enrollment_enrolled_at")) - F.unix_timestamp(F.col("enrolled_at_week"))) / 3600, 1).alias("hours_enrolled_at_week_to_enrollment_enrolled_at"),
        (F.col("score") + F.col("enrollment_score")).alias("score_plus_enrollment_score"),
    )
    .orderBy("enrolled_at_week", "id", "enrollment_id")
    .limit(20)
)
