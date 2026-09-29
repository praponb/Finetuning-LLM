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

pass_by_course = (
    df_enrollments
    .filter(F.col("grade_status") == "PASS")
    .groupBy("course_id")
    .agg(
        F.count("*").alias("pass_count"),
        F.round(F.avg("score"), 2).alias("pass_avg_score"),
    )
)

result = (
    withdrawn_by_course.alias("a")
    .join(pass_by_course.alias("b"), F.col("a.course_id") == F.col("b.course_id"), "full")
    .select(
        F.coalesce(F.col("a.course_id"), F.col("b.course_id")).alias("course_id"),
        F.coalesce(F.col("a.withdrawn_count"), F.lit(0)).alias("withdrawn_count"),
        F.col("a.withdrawn_total_score"),
        F.coalesce(F.col("b.pass_count"), F.lit(0)).alias("pass_count"),
        F.col("b.pass_avg_score"),
        (F.coalesce(F.col("a.withdrawn_count"), F.lit(0)) - F.coalesce(F.col("b.pass_count"), F.lit(0))).alias("withdrawn_count_minus_pass_count"),
    )
    .orderBy("course_id")
)
