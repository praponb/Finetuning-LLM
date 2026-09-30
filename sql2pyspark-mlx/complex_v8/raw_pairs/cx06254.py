from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_student_id = Window.partitionBy("course_id", "student_id")
w_enrolled_at_dayofweek_by_avg = Window.partitionBy("enrolled_at_dayofweek").orderBy(F.col("avg_enrollment_score").desc(), "id")

valid_enrollments = (
    df_enrollments
    .filter((F.col("course_id") <= 9) & (F.col("enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
        F.abs(F.col("score") - 44.4).alias("score_distance"),
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("score").alias("max_enrollment_score"),
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

summarized_rows = (
    valid_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.max_enrollment_score",
        "e.avg_enrollment_score",
        "e.last_enrollment_enrolled_at",
    )
)

sequenced = (
    summarized_rows
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "score_distance",
        "enrolled_at_dayofweek",
        "max_enrollment_score",
        "avg_enrollment_score",
        "last_enrollment_enrolled_at",
        F.max("max_enrollment_score").over(w_course_id_student_id).alias("course_id_student_id_max_max_enrollment_score"),
        F.row_number().over(w_enrolled_at_dayofweek_by_avg).alias("rn"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.sum("score").alias("total_enrollment_score"))
)

profiled = (
    sequenced.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.total_enrollment_score")
)

filtered = (
    profiled.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
)

joined = (
    filtered.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.enrolled_on") >= F.lit("2024-10-09").cast("date"))
    .select(
        "s.id",
        "s.student_id",
        "s.term",
        "s.score",
        "s.score_distance",
        "s.max_enrollment_score",
        "s.avg_enrollment_score",
        "s.last_enrollment_enrolled_at",
        "s.rn",
        "s.total_enrollment_score",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

result = (
    joined
    .select(
        "id",
        "score_distance",
        "avg_enrollment_score",
        "rn",
        "total_enrollment_score",
        "student_enrolled_on",
        "student_cohort",
        "student_is_international",
    )
    .orderBy(F.col("student_cohort").desc(), F.col("score_distance").desc())
)
