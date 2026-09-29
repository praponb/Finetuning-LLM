from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cohort_len_tier = Window.partitionBy("cohort_len_tier")
w_major_or_unknown_cohort_len = Window.partitionBy("major_or_unknown", "cohort_len_tier").orderBy("cohort_len_tier_sum_cohort_len", "id")

valid = (
    df_students
    .filter(F.upper(F.trim(F.col("student_name"))).like("H%"))
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        F.date_trunc("quarter", F.col("enrolled_on")).alias("enrolled_on_quarter"),
    )
)

labeled = (
    valid
    .filter(F.col("enrolled_on_quarter") >= F.lit("2025-01-13 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.length(F.trim(F.col("cohort"))).alias("cohort_len"),
        F.initcap(F.trim(F.col("cohort"))).alias("cohort_clean"),
        F.concat(F.col("major"), F.lit("#"), F.col("id").cast("string")).alias("major_ref"),
    )
)

derived = (
    labeled
    .select(
        "id",
        "major",
        "cohort_len",
        F.coalesce(F.col("major"), F.lit("UNKNOWN")).alias("major_or_unknown"),
        F.when(F.col("cohort_len") >= 12, "A").when(F.col("cohort_len") >= 6, "B").otherwise("C").alias("cohort_len_tier"),
    )
)

sequenced = (
    derived
    .select(
        "id",
        "major_or_unknown",
        "cohort_len_tier",
        F.sum("cohort_len").over(w_cohort_len_tier).alias("cohort_len_tier_sum_cohort_len"),
        F.min("cohort_len").over(w_cohort_len_tier).alias("cohort_len_tier_min_cohort_len"),
    )
)

with_windows = (
    sequenced
    .select(
        "*",
        F.round(F.cume_dist().over(w_major_or_unknown_cohort_len), 3).alias("cohort_len_tier_sum_cohort_len_cume_dist"),
        F.rank().over(w_major_or_unknown_cohort_len).alias("cohort_len_tier_sum_cohort_len_rank"),
        F.round(F.percent_rank().over(w_major_or_unknown_cohort_len), 3).alias("cohort_len_tier_sum_cohort_len_percent_rank"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.min("enrolled_at").alias("first_enrollment_enrolled_at"))
)

with_stats = (
    with_windows.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select("s.*", "e.first_enrollment_enrolled_at")
)

with_children = (
    with_stats.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.cohort_len_tier_min_cohort_len",
        "s.cohort_len_tier_sum_cohort_len_cume_dist",
        "s.cohort_len_tier_sum_cohort_len_rank",
        "s.cohort_len_tier_sum_cohort_len_percent_rank",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

result = (
    with_children
    .rollup("enrollment_course_id", "enrollment_grade_status")
    .agg(
        F.sum(F.when(F.col("enrollment_grade_status") == "FAIL", 1).otherwise(0)).alias("n_enrollment_grade_status_fail"),
        F.min("cohort_len_tier_sum_cohort_len_rank").alias("min_cohort_len_tier_sum_cohort_len_rank"),
        F.countDistinct("enrollment_grade_status").alias("distinct_enrollment_grade_status"),
    )
)
