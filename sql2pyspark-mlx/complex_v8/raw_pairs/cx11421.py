from pyspark.sql import functions as F

withdrawn_monthly = (
    df_enrollments
    .filter(F.col("grade_status") == "WITHDRAWN")
    .groupBy(F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"))
    .agg(
        F.count("*").alias("withdrawn_count"),
        F.sum("score").alias("withdrawn_total_score"),
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
    withdrawn_monthly.alias("a")
    .join(fail_monthly.alias("b"), F.col("a.enrolled_at_month") == F.col("b.enrolled_at_month"), "full")
    .select(
        F.coalesce(F.col("a.enrolled_at_month"), F.col("b.enrolled_at_month")).alias("enrolled_at_month"),
        F.coalesce(F.col("a.withdrawn_count"), F.lit(0)).alias("withdrawn_count"),
        F.col("a.withdrawn_total_score"),
        F.coalesce(F.col("b.fail_count"), F.lit(0)).alias("fail_count"),
        F.coalesce(F.col("b.fail_total_score"), F.lit(0)).alias("fail_total_score"),
        (F.coalesce(F.col("a.withdrawn_count"), F.lit(0)) - F.coalesce(F.col("b.fail_count"), F.lit(0))).alias("withdrawn_count_minus_fail_count"),
    )
    .orderBy("enrolled_at_month")
)
