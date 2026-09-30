from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_on_plus_6m_month_en = Window.partitionBy(F.date_trunc("month", "enrolled_on_plus_6m"), "enrolled_on_year").orderBy("enrolled_on_plus_6m")
w_cohort = Window.partitionBy("cohort")
w_enrolled_on_plus_6m_month_en_2 = Window.partitionBy(F.date_trunc("month", "enrolled_on_plus_6m"), "enrolled_on_year").orderBy("enrolled_on_plus_6m", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_on_plus_6m_month_en_3 = Window.partitionBy(F.date_trunc("month", "enrolled_on_plus_6m"), "enrolled_on_year").orderBy("enrolled_on_plus_6m", "id").rowsBetween(-2, Window.currentRow)
w_enrolled_on_plus_6m_dense_ra = Window.partitionBy("enrolled_on_plus_6m_dense_rank", "enrolled_on_year")

filtered_students = (
    df_students
    .filter((~F.col("is_international")) & (F.col("cohort") != "C2022"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.add_months(F.col("enrolled_on"), 6).alias("enrolled_on_plus_6m"),
        F.substring(F.trim(F.col("major")), 1, 2).alias("major_prefix"),
        F.year(F.col("enrolled_on")).alias("enrolled_on_year"),
    )
)

windowed = (
    filtered_students
    .select(
        "*",
        F.dense_rank().over(w_enrolled_on_plus_6m_month_en).alias("enrolled_on_plus_6m_dense_rank"),
        F.round(F.col("enrolled_on_year") / F.nullif(F.sum("enrolled_on_year").over(w_cohort), F.lit(0)), 4).alias("enrolled_on_year_share_of_cohort"),
        F.round(F.col("gpa") / F.nullif(F.sum("gpa").over(w_cohort), F.lit(0)), 4).alias("gpa_share_of_cohort"),
        F.last("enrolled_on_year").over(w_enrolled_on_plus_6m_month_en_2).alias("last_enrolled_on_year"),
        F.round(F.avg("gpa").over(w_enrolled_on_plus_6m_month_en_3), 2).alias("gpa_avg_3"),
    )
)

annotated = (
    windowed
    .filter(F.col("cohort") != "C2023")
    .select(
        "*",
        F.round(F.col("gpa_share_of_cohort") / F.nullif(F.col("gpa_avg_3"), F.lit(0)), 3).alias("gpa_share_of_cohort_per_gpa_avg_3"),
        F.datediff(F.col("enrolled_on_plus_6m"), F.col("enrolled_on")).alias("days_enrolled_on_to_enrolled_on_plus_6m"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

profiled = (
    annotated.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.min_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

ranked = (
    profiled
    .select(
        "id",
        "student_name",
        "gpa",
        "is_international",
        "major_prefix",
        "enrolled_on_plus_6m_dense_rank",
        "enrolled_on_year_share_of_cohort",
        "gpa_share_of_cohort",
        "last_enrolled_on_year",
        "gpa_avg_3",
        "gpa_share_of_cohort_per_gpa_avg_3",
        "days_enrolled_on_to_enrolled_on_plus_6m",
        "min_enrollment_score",
        "enrollment_count",
        F.sum("enrolled_on_year").over(w_enrolled_on_plus_6m_dense_ra).alias("enrolled_on_plus_6m_dense_rank_enrolled_on_year_"),
        F.min("gpa").over(w_enrolled_on_plus_6m_dense_ra).alias("enrolled_on_plus_6m_dense_rank_enrolled_on_year__2"),
    )
)

scored = (
    ranked
    .filter(F.col("major_prefix").isNotNull())
    .withColumn("gpa_avg_3_per_enrollment_count", F.round(F.col("gpa_avg_3") / F.nullif(F.col("enrollment_count"), F.lit(0)), 3))
)

result = (
    scored
    .filter(~F.col("is_international"))
    .select(
        "id",
        "gpa",
        "is_international",
        "major_prefix",
        "enrolled_on_plus_6m_dense_rank",
        "enrolled_on_year_share_of_cohort",
        "last_enrolled_on_year",
        "gpa_avg_3",
        "gpa_share_of_cohort_per_gpa_avg_3",
        "days_enrolled_on_to_enrolled_on_plus_6m",
        "min_enrollment_score",
        "enrollment_count",
        "enrolled_on_plus_6m_dense_rank_enrolled_on_year_",
        "enrolled_on_plus_6m_dense_rank_enrolled_on_year__2",
        "gpa_avg_3_per_enrollment_count",
    )
    .orderBy("enrollment_count", "min_enrollment_score", "id")
    .limit(10)
)
