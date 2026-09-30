from pyspark.sql import functions as F

withdrawn_by_course = (
    df_enrollments
    .filter(F.col("grade_status") == "WITHDRAWN")
    .groupBy("course_id")
    .agg(
        F.count("*").alias("withdrawn_count"),
        F.sum("score").alias("withdrawn_total_score"),
    )
)

fail_by_course = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
    .groupBy("course_id")
    .agg(
        F.count("*").alias("fail_count"),
        F.sum("score").alias("fail_total_score"),
    )
)

result = (
    withdrawn_by_course.alias("a")
    .join(fail_by_course.alias("b"), F.col("a.course_id") == F.col("b.course_id"), "full")
    .select(
        F.coalesce(F.col("a.course_id"), F.col("b.course_id")).alias("course_id"),
        F.coalesce(F.col("a.withdrawn_count"), F.lit(0)).alias("withdrawn_count"),
        F.coalesce(F.col("a.withdrawn_total_score"), F.lit(0)).alias("withdrawn_total_score"),
        F.coalesce(F.col("b.fail_count"), F.lit(0)).alias("fail_count"),
        F.col("b.fail_total_score"),
        (F.coalesce(F.col("a.withdrawn_count"), F.lit(0)) - F.coalesce(F.col("b.fail_count"), F.lit(0))).alias("withdrawn_count_minus_fail_count"),
    )
    .orderBy("course_id")
)
