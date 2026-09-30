from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrollment_enrolled_at_month = Window.partitionBy(F.month("enrollment_enrolled_at"), "enrollment_grade_status").orderBy("enrolled_on", "id", "enrollment_id")
w_all_by_enrolled_on_running = Window.orderBy(F.col("enrolled_on").desc(), "id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_on = Window.orderBy(F.col("enrolled_on").desc(), "id", "enrollment_id")
w_enrollment_enrolled_at_month_2 = Window.partitionBy(F.month("enrollment_enrolled_at"), "enrollment_grade_status").orderBy("enrolled_on", "id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_international_enrollment = Window.partitionBy("is_international", "enrollment_id")

filtered = (
    df_students
    .filter((F.col("major").isin("Art", "Math", "CS")) & (~F.col("is_international")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.upper(F.trim(F.col("major"))).alias("major_clean"),
    )
)

exploded_detail_enrollments = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.cohort",
        "s.enrolled_on",
        "s.is_international",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
    )
)

qualified = (
    exploded_detail_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (~F.col("e.term").isin("2024T2", "2024T1")), "left_anti")
    .filter(F.col("enrolled_on") < F.lit("2025-06-27").cast("date"))
)

qualified_2 = (
    qualified.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "left_semi")
)

with_trends = (
    qualified_2
    .select(
        "id",
        "cohort",
        "enrolled_on",
        "is_international",
        "enrollment_id",
        "enrollment_grade_status",
        "enrollment_enrolled_at",
        "enrollment_term",
        F.round(F.percent_rank().over(w_enrollment_enrolled_at_month), 3).alias("enrolled_on_percent_rank"),
        F.last("enrollment_score", ignorenulls=True).over(w_all_by_enrolled_on_running).alias("last_known_enrollment_score"),
        F.lag("enrolled_on", 2).over(w_all_by_enrolled_on).alias("prev2_enrolled_on"),
        F.last("enrollment_enrolled_at", ignorenulls=True).over(w_enrollment_enrolled_at_month_2).alias("last_known_enrollment_enrolled_at"),
        (F.col("enrollment_score") - F.lag("enrollment_score").over(w_enrollment_enrolled_at_month)).alias("enrollment_score_change"),
        F.size(F.collect_set("cohort").over(w_is_international_enrollment)).alias("distinct_cohort_in_is_international_enrollment_i"),
    )
)

result = (
    with_trends
    .filter(F.col("prev2_enrolled_on") >= F.lit("2024-04-04").cast("date"))
    .select(
        "id",
        "cohort",
        "enrolled_on",
        "is_international",
        "enrollment_id",
        "enrollment_grade_status",
        "enrollment_enrolled_at",
        "enrolled_on_percent_rank",
        "prev2_enrolled_on",
        "last_known_enrollment_enrolled_at",
        "enrollment_score_change",
        "distinct_cohort_in_is_international_enrollment_i",
        F.substring(F.trim(F.col("enrollment_grade_status")), 1, 4).alias("enrollment_grade_status_prefix"),
        F.year(F.col("enrolled_on")).alias("enrolled_on_year"),
    )
    .orderBy(
        F.col("enrollment_id").desc(),
        "distinct_cohort_in_is_international_enrollment_i",
    )
)
