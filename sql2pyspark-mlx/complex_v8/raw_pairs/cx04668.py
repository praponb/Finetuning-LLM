from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrollment_term_by_enrolled = Window.partitionBy("enrollment_term").orderBy("enrolled_at", "id", "enrollment_id")
w_enrollment_term_by_enrolled_2 = Window.partitionBy("enrollment_term").orderBy("enrolled_at")

recent = (
    df_enrollments
    .filter(F.col("score") <= 90.9)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws("|", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

exploded_detail = (
    recent.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_clean",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
    )
)

with_windows = (
    exploded_detail
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "grade_status_clean",
        "enrollment_id",
        "enrollment_student_id",
        "enrollment_grade_status",
        "enrollment_term",
        F.row_number().over(w_enrollment_term_by_enrolled).alias("enrolled_at_seq"),
        F.dense_rank().over(w_enrollment_term_by_enrolled_2).alias("enrolled_at_dense_rank"),
        F.round(F.cume_dist().over(w_enrollment_term_by_enrolled), 3).alias("enrolled_at_cume_dist"),
        F.round(F.cume_dist().over(w_enrollment_term_by_enrolled_2), 3).alias("enrolled_at_cume_dist_2"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.count(F.when(F.col("grade_status") != "WITHDRAWN", 1)).alias("enrollment_grade_status_count"),
    )
)

profiled = (
    with_windows.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.enrollment_student_id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at",
        "e.first_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

result = (
    profiled
    .select(
        "id",
        "enrollment_id",
        F.col("enrolled_at_cume_dist").alias("m_enrolled_at_cume_dist"),
        F.col("enrolled_at_cume_dist_2").alias("m_enrolled_at_cume_dist_2"),
    )
    .unpivot(
        ids=["id", "enrollment_id"],
        values=["m_enrolled_at_cume_dist", "m_enrolled_at_cume_dist_2"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
