from pyspark.sql import functions as F

withdrawn_by_student = (
    df_enrollments
    .filter(F.col("grade_status") == "WITHDRAWN")
    .groupBy("student_id")
    .agg(
        F.count("*").alias("withdrawn_count"),
        F.sum("score").alias("withdrawn_total_score"),
    )
)

fail_by_student = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
    .groupBy("student_id")
    .agg(
        F.count("*").alias("fail_count"),
        F.sum("score").alias("fail_total_score"),
    )
)

result = (
    withdrawn_by_student.alias("a")
    .join(fail_by_student.alias("b"), F.col("a.student_id") == F.col("b.student_id"), "full")
    .select(
        F.coalesce(F.col("a.student_id"), F.col("b.student_id")).alias("student_id"),
        F.coalesce(F.col("a.withdrawn_count"), F.lit(0)).alias("withdrawn_count"),
        F.coalesce(F.col("a.withdrawn_total_score"), F.lit(0)).alias("withdrawn_total_score"),
        F.coalesce(F.col("b.fail_count"), F.lit(0)).alias("fail_count"),
        F.col("b.fail_total_score"),
        (F.coalesce(F.col("a.withdrawn_count"), F.lit(0)) - F.coalesce(F.col("b.fail_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("student_id")
)
