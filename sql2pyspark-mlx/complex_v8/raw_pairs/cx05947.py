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

withdrawn_monthly = (
    df_enrollments
    .filter(F.col("grade_status") == "WITHDRAWN")
    .groupBy(F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"))
    .agg(
        F.count("*").alias("withdrawn_count"),
        F.sum("score").alias("withdrawn_total_score"),
    )
)

result = (
    fail_monthly.alias("a")
    .join(withdrawn_monthly.alias("b"), F.col("a.enrolled_at_month") == F.col("b.enrolled_at_month"), "full")
    .select(
        F.coalesce(F.col("a.enrolled_at_month"), F.col("b.enrolled_at_month")).alias("enrolled_at_month"),
        F.coalesce(F.col("a.fail_count"), F.lit(0)).alias("fail_count"),
        F.col("a.fail_total_score"),
        F.coalesce(F.col("b.withdrawn_count"), F.lit(0)).alias("withdrawn_count"),
        F.col("b.withdrawn_total_score"),
        (F.coalesce(F.col("a.fail_count"), F.lit(0)) - F.coalesce(F.col("b.withdrawn_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("enrolled_at_month")
)
