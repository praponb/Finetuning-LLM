from pyspark.sql import functions as F

fail_by_course = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
    .groupBy("course_id")
    .agg(
        F.count("*").alias("fail_count"),
        F.round(F.avg("score"), 2).alias("fail_avg_score"),
    )
)

withdrawn_by_course = (
    df_enrollments
    .filter(F.col("grade_status") == "WITHDRAWN")
    .groupBy("course_id")
    .agg(
        F.count("*").alias("withdrawn_count"),
        F.round(F.avg("score"), 2).alias("withdrawn_avg_score"),
    )
)

result = (
    fail_by_course.alias("a")
    .join(withdrawn_by_course.alias("b"), F.col("a.course_id") == F.col("b.course_id"), "full")
    .select(
        F.coalesce(F.col("a.course_id"), F.col("b.course_id")).alias("course_id"),
        F.coalesce(F.col("a.fail_count"), F.lit(0)).alias("fail_count"),
        F.col("a.fail_avg_score"),
        F.coalesce(F.col("b.withdrawn_count"), F.lit(0)).alias("withdrawn_count"),
        F.col("b.withdrawn_avg_score"),
        (F.coalesce(F.col("a.fail_count"), F.lit(0)) - F.coalesce(F.col("b.withdrawn_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("course_id")
)
