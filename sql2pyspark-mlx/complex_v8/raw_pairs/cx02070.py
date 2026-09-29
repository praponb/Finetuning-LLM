from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at_hour", "id")
w_grade_status_enrolled_at_hou = Window.partitionBy("grade_status", F.year("enrolled_at_hour")).orderBy(F.col("enrolled_at_hour").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status = Window.partitionBy("grade_status")
w_grade_status_enrolled_at_hou_2 = Window.partitionBy("grade_status", F.year("enrolled_at_hour")).orderBy(F.col("enrolled_at_hour").desc(), "id")

recent = (
    df_enrollments
    .filter(F.col("course_id") >= 4)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("hour", F.col("enrolled_at")).alias("enrolled_at_hour"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_trends = (
    recent
    .select(
        "*",
        F.lead("score_filled", 2).over(w_grade_status_by_enrolled_at).alias("next2_score_filled"),
        F.nth_value("enrolled_at_hour", 2).over(w_grade_status_enrolled_at_hou).alias("second_enrolled_at_hour"),
        F.size(F.collect_set("student_id").over(w_grade_status)).alias("distinct_student_id_in_grade_status"),
        F.lead("enrolled_at_hour", 2).over(w_grade_status_enrolled_at_hou_2).alias("next2_enrolled_at_hour"),
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
    )
)

course_id_stats = (
    with_trends
    .groupBy("course_id")
    .agg(F.avg("rows_in_grade_status").alias("avg_rows_in_grade_status"))
)

compared = (
    with_trends.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.rows_in_grade_status") > F.col("g.avg_rows_in_grade_status"))
    .select(
        "s.*",
        "g.avg_rows_in_grade_status",
        F.round(F.col("s.rows_in_grade_status") - F.col("g.avg_rows_in_grade_status"), 2).alias("rows_in_grade_status_minus_avg"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
    )
)

with_stats = (
    compared.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

result = (
    with_stats
    .groupingSets(
        [["enrollment_count_2", "student_id", "grade_status"], ["enrollment_count_2"], ["student_id"], []],
        "enrollment_count_2", "student_id", "grade_status",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
    )
)
