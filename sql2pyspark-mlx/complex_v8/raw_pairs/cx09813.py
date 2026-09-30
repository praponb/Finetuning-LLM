from pyspark.sql import functions as F

pass_by_course = (
    df_enrollments
    .filter(F.col("grade_status") == "PASS")
    .groupBy("course_id")
    .agg(
        F.count("*").alias("pass_count"),
        F.sum("score").alias("pass_total_score"),
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
    pass_by_course.alias("a")
    .join(fail_by_course.alias("b"), F.col("a.course_id") == F.col("b.course_id"), "full")
    .select(
        F.coalesce(F.col("a.course_id"), F.col("b.course_id")).alias("course_id"),
        F.coalesce(F.col("a.pass_count"), F.lit(0)).alias("pass_count"),
        F.col("a.pass_total_score"),
        F.coalesce(F.col("b.fail_count"), F.lit(0)).alias("fail_count"),
        F.coalesce(F.col("b.fail_total_score"), F.lit(0)).alias("fail_total_score"),
        (F.coalesce(F.col("a.pass_count"), F.lit(0)) - F.coalesce(F.col("b.fail_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("course_id")
)
