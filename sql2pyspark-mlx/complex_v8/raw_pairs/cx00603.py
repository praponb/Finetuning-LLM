from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_term_by_enrolled_a = Window.partitionBy("course_id", "term").orderBy("enrolled_at", "id")
w_term_or_unknown_course_id = Window.partitionBy("term_or_unknown", "course_id")
w_course_id_term_by_enrolled_a_2 = Window.partitionBy("course_id", "term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_enrollments
    .filter(F.col("term").isin("2024T1", "2025T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "term_or_unknown",
        F.ntile(5).over(w_course_id_term_by_enrolled_a).alias("enrolled_at_quintile"),
        F.min("days_since_enrolled_at").over(w_term_or_unknown_course_id).alias("term_or_unknown_course_id_min_days_since_enrolle"),
        F.min("score").over(w_term_or_unknown_course_id).alias("term_or_unknown_course_id_min_score"),
        F.round(F.col("days_since_enrolled_at") - F.avg("days_since_enrolled_at").over(w_term_or_unknown_course_id), 2).alias("days_since_enrolled_at_vs_term_or_unknown_course"),
        F.nth_value("score", 2).over(w_course_id_term_by_enrolled_a_2).alias("second_score"),
    )
)

without_matches = (
    with_windows.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
    .filter(F.col("grade_status").isNotNull())
)

qualified = (
    without_matches.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") > 5.7) & (F.col("e.term").isNotNull()), "left_anti")
    .filter(F.col("course_id").isNotNull())
)

result = (
    qualified
    .groupBy("enrolled_at_quintile")
    .pivot("grade_status", ["FAIL", "WITHDRAWN"])
    .agg(F.max("term_or_unknown_course_id_min_days_since_enrolle"))
    .orderBy("enrolled_at_quintile")
)
