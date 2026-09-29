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

withdrawn_monthly = (
    df_enrollments
    .filter(F.col("grade_status") == "WITHDRAWN")
    .groupBy(F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"))
    .agg(
        F.count("*").alias("withdrawn_count"),
        F.round(F.avg("score"), 2).alias("withdrawn_avg_score"),
    )
)

result = (
    pass_monthly.alias("a")
    .join(withdrawn_monthly.alias("b"), F.col("a.enrolled_at_month") == F.col("b.enrolled_at_month"), "full")
    .select(
        F.coalesce(F.col("a.enrolled_at_month"), F.col("b.enrolled_at_month")).alias("enrolled_at_month"),
        F.coalesce(F.col("a.pass_count"), F.lit(0)).alias("pass_count"),
        F.col("a.pass_total_score"),
        F.coalesce(F.col("b.withdrawn_count"), F.lit(0)).alias("withdrawn_count"),
        F.col("b.withdrawn_avg_score"),
        (F.coalesce(F.col("a.pass_count"), F.lit(0)) - F.coalesce(F.col("b.withdrawn_count"), F.lit(0))).alias("pass_count_minus_withdrawn_count"),
    )
    .orderBy("enrolled_at_month")
)
