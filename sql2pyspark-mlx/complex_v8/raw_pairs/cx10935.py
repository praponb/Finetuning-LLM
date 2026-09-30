from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_student_major_term_by_enroll = Window.partitionBy("student_major", "term").orderBy(F.col("enrollment_count").desc())
w_all_by_student_enrolled_on = Window.orderBy(F.col("student_enrolled_on").asc_nulls_last(), "id")

base_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) | (F.col("enrolled_at") < F.lit("2025-02-12 12:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.concat_ws("|", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label_2"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
    )
)

windowed = (
    base_enrollments
    .select(
        "*",
        F.lead("enrolled_at").over(w_all_by_enrolled_at).alias("next_enrolled_at"),
        F.ntile(5).over(w_all_by_enrolled_at).alias("enrolled_at_quintile"),
    )
)

enriched_student = (
    windowed.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.grade_status_term_label",
        "s.grade_status_term_label_2",
        "s.next_enrolled_at",
        "s.enrolled_at_quintile",
        F.col("st.is_international").alias("student_is_international"),
        "st.student_name",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.count("*").alias("enrollment_count"))
)

with_stats = (
    enriched_student.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

joined = (
    with_stats.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.grade_status_term_label",
        "s.grade_status_term_label_2",
        "s.next_enrolled_at",
        "s.student_name",
        "s.enrollment_count",
        F.col("st.major").alias("student_major"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "grade_status_term_label",
        "grade_status_term_label_2",
        "student_name",
        "enrollment_count",
        "student_major",
        "student_enrolled_on",
        F.dense_rank().over(w_student_major_term_by_enroll).alias("enrollment_count_dense_rank"),
        F.lag("enrollment_count").over(w_all_by_student_enrolled_on).alias("prev_enrollment_count"),
    )
)

result = (
    metrics
    .cube("student_id", "course_id")
    .agg(
        F.max("enrollment_count_dense_rank").alias("max_enrollment_count_dense_rank"),
        F.sum("enrollment_count_dense_rank").alias("total_enrollment_count_dense_rank"),
    )
)
