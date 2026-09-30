from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_year = Window.partitionBy(F.year("enrolled_at"))
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id")
w_grade_status_or_unknown_2_by = Window.partitionBy("grade_status_or_unknown_2").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-02-06 09:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown_2"),
    )
)

with_trends = (
    recent
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "days_since_enrolled_at",
        "grade_status_or_unknown",
        F.round(F.avg("score").over(w_enrolled_at_year), 2).alias("enrolled_at_year_avg_score"),
        F.lead("enrolled_at").over(w_grade_status_by_score).alias("next_enrolled_at"),
        F.nth_value("score", 2).over(w_grade_status_or_unknown_2_by).alias("second_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_year), F.lit(0)), 4).alias("score_share_of_enrolled_at_year"),
    )
)

joined = (
    with_trends.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.days_since_enrolled_at",
        "s.grade_status_or_unknown",
        "s.enrolled_at_year_avg_score",
        "s.next_enrolled_at",
        "s.second_score",
        "s.score_share_of_enrolled_at_year",
        "c.course_code",
    )
)

enriched = (
    joined.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.*",
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
    )
)

combined_details = (
    enriched.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.dept").isin("Art", "CS", "Math"))
    .select(
        "s.id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.days_since_enrolled_at",
        "s.grade_status_or_unknown",
        "s.enrolled_at_year_avg_score",
        "s.next_enrolled_at",
        "s.score_share_of_enrolled_at_year",
        "s.course_code",
        F.col("c.dept").alias("course_dept_2"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "term",
        "score",
        "days_since_enrolled_at",
        "grade_status_or_unknown",
        "enrolled_at_year_avg_score",
        "score_share_of_enrolled_at_year",
        "course_code",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at_2"),
        F.length(F.trim(F.col("course_dept_2"))).alias("course_dept_2_len"),
    )
    .orderBy(F.col("days_since_enrolled_at").asc_nulls_last())
)
