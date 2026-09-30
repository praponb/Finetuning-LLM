from pyspark.sql import functions as F

pass_monthly = (
    df_enrollments
    .filter(F.col("grade_status") == "PASS")
    .groupBy(F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"))
    .agg(
        F.count("*").alias("pass_count"),
        F.sum("score").alias("pass_total_score"),
    )
)

fail_monthly = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
    .groupBy(F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"))
    .agg(
        F.count("*").alias("fail_count"),
        F.sum("score").alias("fail_total_score"),
    )
)

result = (
    pass_monthly.alias("a")
    .join(fail_monthly.alias("b"), F.col("a.enrolled_at_month") == F.col("b.enrolled_at_month"), "full")
    .select(
        F.coalesce(F.col("a.enrolled_at_month"), F.col("b.enrolled_at_month")).alias("enrolled_at_month"),
        F.coalesce(F.col("a.pass_count"), F.lit(0)).alias("pass_count"),
        F.col("a.pass_total_score"),
        F.coalesce(F.col("b.fail_count"), F.lit(0)).alias("fail_count"),
        F.col("b.fail_total_score"),
        (F.coalesce(F.col("a.pass_count"), F.lit(0)) - F.coalesce(F.col("b.fail_count"), F.lit(0))).alias("pass_count_minus_fail_count"),
    )
    .orderBy("enrolled_at_month")
)
