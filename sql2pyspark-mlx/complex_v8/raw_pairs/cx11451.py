from pyspark.sql import functions as F

fail_by_student = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
    .groupBy("student_id")
    .agg(
        F.count("*").alias("fail_count"),
        F.round(F.avg("score"), 2).alias("fail_avg_score"),
    )
)

withdrawn_by_student = (
    df_enrollments
    .filter(F.col("grade_status") == "WITHDRAWN")
    .groupBy("student_id")
    .agg(
        F.count("*").alias("withdrawn_count"),
        F.sum("score").alias("withdrawn_total_score"),
    )
)

result = (
    fail_by_student.alias("a")
    .join(withdrawn_by_student.alias("b"), F.col("a.student_id") == F.col("b.student_id"), "full")
    .select(
        F.coalesce(F.col("a.student_id"), F.col("b.student_id")).alias("student_id"),
        F.coalesce(F.col("a.fail_count"), F.lit(0)).alias("fail_count"),
        F.coalesce(F.col("a.fail_avg_score"), F.lit(0)).alias("fail_avg_score"),
        F.coalesce(F.col("b.withdrawn_count"), F.lit(0)).alias("withdrawn_count"),
        F.col("b.withdrawn_total_score"),
        (F.coalesce(F.col("a.fail_count"), F.lit(0)) - F.coalesce(F.col("b.withdrawn_count"), F.lit(0))).alias("fail_count_minus_withdrawn_count"),
    )
    .orderBy("student_id")
)
