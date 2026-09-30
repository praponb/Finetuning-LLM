from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_enrolled_at = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc(), "id")
w_course_id_by_enrolled_at_ful = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status = Window.partitionBy("grade_status")
w_all_by_enrolled_at_full = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_month = Window.orderBy(F.col("enrolled_at_month").desc(), "id")

base = (
    df_enrollments
    .filter(F.col("term") == "2024T2")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown_2"),
    )
)

sequenced = (
    base
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "term_or_unknown_2",
        F.row_number().over(w_course_id_by_enrolled_at).alias("rn"),
        F.first("enrolled_at").over(w_course_id_by_enrolled_at_ful).alias("first_enrolled_at"),
    )
)

derived = (
    sequenced
    .select(
        "id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "term_or_unknown_2",
        "rn",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.month(F.col("first_enrolled_at")).alias("first_enrolled_at_month"),
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
    )
)

with_trends = (
    derived
    .select(
        "id",
        "course_id",
        "grade_status",
        "term_or_unknown_2",
        "rn",
        "first_enrolled_at_month",
        "enrolled_at_month",
        F.round(F.avg("first_enrolled_at_month").over(w_grade_status), 2).alias("grade_status_avg_first_enrolled_at_month"),
        F.nth_value("enrolled_at_month", 2).over(w_all_by_enrolled_at_full).alias("second_enrolled_at_month"),
        F.dense_rank().over(w_all_by_enrolled_at_month).alias("enrolled_at_month_dense_rank"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.min("score").alias("min_enrollment_score"),
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
    )
)

profiled = (
    with_trends.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.min_enrollment_score",
        "e.avg_enrollment_score",
    )
)

top_per_group = (
    profiled
    .filter((F.col("rn") <= 5) & (F.col("course_id") <= 8))
    .drop("rn")
)

result = (
    top_per_group
    .groupBy("enrolled_at_month")
    .pivot("grade_status", ["WITHDRAWN", "INCOMPLETE", "FAIL"])
    .agg(F.min("second_enrolled_at_month").alias("min_v"), F.count("second_enrolled_at_month").alias("n"))
    .orderBy("enrolled_at_month")
)
