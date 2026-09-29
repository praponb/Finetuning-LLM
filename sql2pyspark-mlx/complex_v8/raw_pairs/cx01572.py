from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_month_month_by_e = Window.partitionBy(F.date_trunc("month", "enrolled_at_month")).orderBy("enrolled_at", "id").rowsBetween(-1, Window.currentRow)
w_enrolled_at_month_course_id = Window.partitionBy(F.month("enrolled_at"), "course_id")

base = (
    df_enrollments
    .filter((F.col("term") == "2025T1") & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("month", F.col("enrolled_at")).alias("enrolled_at_month"),
        F.abs(F.col("score") - 69.6).alias("score_distance"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

with_counts = (
    base.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

windowed = (
    with_counts
    .select(
        "*",
        F.round(F.avg("score_distance").over(w_enrolled_at_month_month_by_e), 2).alias("score_distance_avg_2"),
        F.round(F.col("score_distance") / F.nullif(F.sum("score_distance").over(w_enrolled_at_month_course_id), F.lit(0)), 4).alias("score_distance_share_of_enrolled_at_month_course"),
    )
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "last_enrollment_enrolled_at",
        "score_distance_share_of_enrolled_at_month_course",
        F.concat_ws("|", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)
