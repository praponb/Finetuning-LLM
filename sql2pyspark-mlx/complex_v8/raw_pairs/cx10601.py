from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term")
w_all_by_enrolled_at_running = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_enrollments = (
    df_enrollments
    .filter(F.col("score").between(17.4, 77.0))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

with_windows = (
    prepared_enrollments
    .select(
        "*",
        F.size(F.collect_set("course_id").over(w_term)).alias("distinct_course_id_in_term"),
        F.round(F.col("days_since_enrolled_at") / F.nullif(F.sum("days_since_enrolled_at").over(w_term), F.lit(0)), 4).alias("days_since_enrolled_at_share_of_term"),
        F.first("enrolled_at", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("first_known_enrolled_at"),
        F.first("days_since_enrolled_at").over(w_all_by_enrolled_at_full).alias("first_days_since_enrolled_at"),
    )
)

result = (
    with_windows
    .cube("term", "student_id", "grade_status")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.avg("distinct_course_id_in_term"), 2).alias("avg_distinct_course_id_in_term"),
        F.percentile_approx("distinct_course_id_in_term", 0.5).alias("median_distinct_course_id_in_term"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
    )
    .select(
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        "student_id",
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "grouping_level",
        "avg_distinct_course_id_in_term",
        "median_distinct_course_id_in_term",
        "distinct_grade_status",
    )
    .orderBy(
        F.col("term").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
    )
)
