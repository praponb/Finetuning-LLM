from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cohort_is_international = Window.partitionBy("cohort", "is_international")
w_major_by_last_enrollment_enr = Window.partitionBy("major").orderBy("last_enrollment_enrolled_at", "id").rowsBetween(-2, -1)
w_major_by_last_enrollment_enr_2 = Window.partitionBy("major").orderBy("last_enrollment_enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_students
    .filter(F.col("major") != "CS")
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.concat(F.col("major"), F.lit("#"), F.col("id").cast("string")).alias("major_ref"),
        F.substring(F.trim(F.col("student_name")), 1, 3).alias("student_name_prefix"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

with_stats = (
    recent.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

with_windows = (
    with_stats
    .select(
        "*",
        F.sum("gpa").over(w_cohort_is_international).alias("cohort_is_international_sum_gpa"),
        F.round(F.avg("gpa").over(w_major_by_last_enrollment_enr), 2).alias("gpa_avg_2_prior"),
        F.sum("gpa").over(w_major_by_last_enrollment_enr_2).alias("running_gpa"),
        F.round(F.avg("gpa").over(w_cohort_is_international), 2).alias("cohort_is_international_avg_gpa"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["is_international", "cohort"], ["is_international"], ["cohort"], []],
        "is_international", "cohort",
    )
    .agg(
        F.min("cohort_is_international_sum_gpa").alias("min_cohort_is_international_sum_gpa"),
    )
)
