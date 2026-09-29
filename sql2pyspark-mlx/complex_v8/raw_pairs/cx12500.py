from pyspark.sql import functions as F

pass_by_student = (
    df_enrollments
    .filter(F.col("grade_status") == "PASS")
    .groupBy("student_id")
    .agg(
        F.count("*").alias("pass_count"),
        F.sum("score").alias("pass_total_score"),
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
    pass_by_student.alias("a")
    .join(withdrawn_by_student.alias("b"), F.col("a.student_id") == F.col("b.student_id"), "full")
    .select(
        F.coalesce(F.col("a.student_id"), F.col("b.student_id")).alias("student_id"),
        F.coalesce(F.col("a.pass_count"), F.lit(0)).alias("pass_count"),
        F.col("a.pass_total_score"),
        F.coalesce(F.col("b.withdrawn_count"), F.lit(0)).alias("withdrawn_count"),
        F.coalesce(F.col("b.withdrawn_total_score"), F.lit(0)).alias("withdrawn_total_score"),
        (F.coalesce(F.col("a.pass_count"), F.lit(0)) - F.coalesce(F.col("b.withdrawn_count"), F.lit(0))).alias("pass_count_minus_withdrawn_count"),
    )
    .orderBy("student_id")
)
