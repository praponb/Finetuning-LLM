from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_on_plus_1m_year_maj = Window.partitionBy(F.year("enrolled_on_plus_1m"), "major")
w_major_cohort_by_enrolled_on = Window.partitionBy("major", "cohort").orderBy(F.col("enrolled_on").desc())
w_all_by_enrolled_on_moving_3 = Window.orderBy("enrolled_on", "id").rowsBetween(-3, Window.currentRow)
w_major_cohort_by_enrolled_on_2 = Window.partitionBy("major", "cohort").orderBy(F.col("enrolled_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_students = (
    df_students
    .filter(F.col("student_name").isNotNull())
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.initcap(F.trim(F.col("cohort"))).alias("cohort_clean"),
        F.add_months(F.col("enrolled_on"), 1).alias("enrolled_on_plus_1m"),
    )
)

ranked = (
    prepared_students
    .select(
        "*",
        F.round(F.col("gpa") / F.nullif(F.sum("gpa").over(w_enrolled_on_plus_1m_year_maj), F.lit(0)), 4).alias("gpa_share_of_enrolled_on_plus_1m_year_major"),
        F.sum("gpa").over(w_enrolled_on_plus_1m_year_maj).alias("enrolled_on_plus_1m_year_major_sum_gpa"),
        F.rank().over(w_major_cohort_by_enrolled_on).alias("enrolled_on_rank"),
        F.sum("gpa").over(w_all_by_enrolled_on_moving_3).alias("gpa_sum_4"),
        F.nth_value("enrolled_on_plus_1m", 2).over(w_major_cohort_by_enrolled_on_2).alias("second_enrolled_on_plus_1m"),
    )
)

labeled = (
    ranked
    .filter(F.col("gpa_sum_4") > 5.2)
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        "enrolled_on_plus_1m",
        "enrolled_on_plus_1m_year_major_sum_gpa",
        "enrolled_on_rank",
        "gpa_sum_4",
        "second_enrolled_on_plus_1m",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_on_plus_1m")).alias("days_since_enrolled_on_plus_1m"),
        F.last_day(F.col("enrolled_on")).alias("enrolled_on_month_end"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("score").alias("max_enrollment_score"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at_2"),
    )
)

with_counts = (
    labeled.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.max_enrollment_score",
        "e.first_enrollment_enrolled_at",
        "e.first_enrollment_enrolled_at_2",
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.min("enrolled_at").alias("first_enrollment_enrolled_at_3"))
)

with_stats = (
    with_counts.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select("s.*", "e.first_enrollment_enrolled_at_3")
)

enriched = (
    with_stats
    .filter(F.col("enrolled_on_rank") > 4)
    .select(
        "*",
        F.upper(F.trim(F.col("major"))).alias("major_clean"),
        F.initcap(F.trim(F.col("major"))).alias("major_clean_2"),
        F.concat(F.col("student_name"), F.lit("#"), F.col("enrolled_on_rank").cast("string")).alias("student_name_ref"),
    )
)

result = (
    enriched
    .groupBy("first_enrollment_enrolled_at_3")
    .pivot("major", ["Art", "Biz", "Bio", "Math"])
    .agg(F.sum("enrolled_on_plus_1m_year_major_sum_gpa"))
    .orderBy("first_enrollment_enrolled_at_3")
)
