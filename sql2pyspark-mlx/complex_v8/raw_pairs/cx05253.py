from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_enrolled_at_qu = Window.partitionBy("student_id").orderBy(F.col("enrolled_at_quarter").asc_nulls_last(), "id")
w_student_id_by_enrolled_at_qu_2 = Window.partitionBy("student_id").orderBy(F.col("enrolled_at_quarter").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_enrollments
    .filter(~F.col("grade_status").isin("WITHDRAWN", "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

scored = (
    filtered
    .filter(F.col("grade_status") != "PASS")
    .select(
        "id",
        "student_id",
        "course_id",
        "score_filled",
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.when(F.col("score") >= 73.0, "HIGH").when(F.col("score") >= 34.2, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

windowed = (
    scored
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at_quarter",
        F.row_number().over(w_student_id_by_enrolled_at_qu).alias("rn"),
        F.round(F.avg("enrolled_at_quarter").over(w_student_id_by_enrolled_at_qu_2), 2).alias("enrolled_at_quarter_avg_4"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("term").isin("2025T1", "2024T2", "2024T1"), 1)).alias("enrollment_term_count"),
        F.count(F.when(F.col("term").isNotNull(), 1)).alias("enrollment_term_count_2"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

profiled = (
    windowed.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        F.coalesce(F.col("e.enrollment_term_count_2"), F.lit(0)).alias("enrollment_term_count_2"),
        "e.last_enrollment_enrolled_at",
    )
)

labeled = (
    profiled
    .select(
        "*",
        F.round(F.col("enrollment_term_count_2") / F.nullif(F.col("enrollment_term_count"), F.lit(0)), 3).alias("enrollment_term_count_2_per_enrollment_term_coun"),
        F.abs(F.col("enrollment_term_count_2") - 3).alias("enrollment_term_count_2_distance"),
        F.round(F.col("rn") / F.nullif(F.col("enrolled_at_quarter"), F.lit(0)), 3).alias("rn_per_enrolled_at_quarter"),
    )
)

result = (
    labeled
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at_quarter",
        "rn",
        "enrolled_at_quarter_avg_4",
        "enrollment_term_count",
        "enrollment_term_count_2",
        "last_enrollment_enrolled_at",
        "enrollment_term_count_2_per_enrollment_term_coun",
        "enrollment_term_count_2_distance",
        "rn_per_enrolled_at_quarter",
        (F.floor(F.col("rn") / 2) * 2).alias("rn_bucket"),
        F.abs(F.col("enrollment_term_count") - 3).alias("enrollment_term_count_distance"),
    )
    .orderBy("enrolled_at_quarter", "id")
    .limit(10)
)
