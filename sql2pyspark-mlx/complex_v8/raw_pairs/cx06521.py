from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cohort_major = Window.partitionBy("cohort", "major")
w_all_by_gpa_prior_6 = Window.orderBy("gpa", "id").rowsBetween(-6, -1)
w_all_by_gpa_full = Window.orderBy("gpa", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_gpa = Window.orderBy("gpa", "id")
w_all_by_gpa_ties = Window.orderBy("gpa")
w_gpa_rank_enrollment_count_by = Window.partitionBy("gpa_rank", "enrollment_count").orderBy("gpa", "id")
w_gpa_rank_cohort = Window.partitionBy("gpa_rank", "cohort")
w_all_by_first_enrollment_enro = Window.orderBy("first_enrollment_enrolled_at", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_students
    .filter(F.col("gpa") < 3.4)
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

screened = (
    valid.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.term") == "2025T2"), "left_semi")
    .filter(F.length(F.trim(F.col("student_name"))) >= 6)
)

metrics = (
    screened
    .select(
        "*",
        F.max("gpa").over(w_all_by_gpa_prior_6).alias("gpa_max_6_prior"),
        F.first("gpa").over(w_all_by_gpa_full).alias("first_gpa"),
        F.round(F.percent_rank().over(w_all_by_gpa), 3).alias("gpa_percent_rank"),
        F.nth_value("enrolled_on", 2).over(w_all_by_gpa_full).alias("second_enrolled_on"),
        F.rank().over(w_all_by_gpa_ties).alias("gpa_rank"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.max("score").alias("max_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

with_counts = (
    metrics.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        "e.max_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

with_trends = (
    with_counts
    .select(
        "*",
        F.lead("gpa_max_6_prior").over(w_gpa_rank_enrollment_count_by).alias("next_gpa_max_6_prior"),
        F.count(F.lit(1)).over(w_gpa_rank_cohort).alias("rows_in_gpa_rank_cohort"),
        F.round(F.avg("gpa_max_6_prior").over(w_all_by_first_enrollment_enro), 2).alias("gpa_max_6_prior_avg_4"),
    )
)

cohort_stats = (
    with_trends
    .groupBy("cohort")
    .agg(F.max("gpa_max_6_prior").alias("max_gpa_max_6_prior"))
)

compared = (
    with_trends.alias("s")
    .join(cohort_stats.alias("g"), F.col("g.cohort") == F.col("s.cohort"), "inner")
    .filter(F.col("s.gpa_max_6_prior") == F.col("g.max_gpa_max_6_prior"))
    .select(
        "s.*",
        "g.max_gpa_max_6_prior",
        F.round(F.col("s.gpa_max_6_prior") - F.col("g.max_gpa_max_6_prior"), 2).alias("gpa_max_6_prior_minus_max"),
    )
)

result = (
    compared
    .groupBy("enrolled_on")
    .pivot("cohort", ["C2021", "C2023"])
    .agg(F.avg("rows_in_gpa_rank_cohort"))
    .orderBy("enrolled_on")
)
