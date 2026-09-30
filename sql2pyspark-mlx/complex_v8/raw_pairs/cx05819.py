from pyspark.sql import functions as F

fail_monthly = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
    .groupBy(F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"))
    .agg(
        F.count("*").alias("fail_count"),
        F.sum("score").alias("fail_total_score"),
    )
)

pass_monthly = (
    df_enrollments
    .filter(F.col("grade_status") == "PASS")
    .groupBy(F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"))
    .agg(
        F.count("*").alias("pass_count"),
        F.round(F.avg("score"), 2).alias("pass_avg_score"),
    )
)

result = (
    fail_monthly.alias("a")
    .join(pass_monthly.alias("b"), F.col("a.enrolled_at_month") == F.col("b.enrolled_at_month"), "full")
    .select(
        F.coalesce(F.col("a.enrolled_at_month"), F.col("b.enrolled_at_month")).alias("enrolled_at_month"),
        F.coalesce(F.col("a.fail_count"), F.lit(0)).alias("fail_count"),
        F.coalesce(F.col("a.fail_total_score"), F.lit(0)).alias("fail_total_score"),
        F.coalesce(F.col("b.pass_count"), F.lit(0)).alias("pass_count"),
        F.coalesce(F.col("b.pass_avg_score"), F.lit(0)).alias("pass_avg_score"),
        (F.coalesce(F.col("a.fail_count"), F.lit(0)) - F.coalesce(F.col("b.pass_count"), F.lit(0))).alias("fail_count_minus_pass_count"),
    )
    .orderBy("enrolled_at_month")
)
