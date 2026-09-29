from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter((F.hour(F.col("enrolled_at")).between(8, 11)) & (F.col("course_id").isNotNull()))
)

rollup_base = (
    recent_enrollments
    .groupBy(
        "student_id",
        "course_id",
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.sum(F.when(F.col("term") == "2025T1", 1).otherwise(0)).alias("n_term_2025t1"),
        F.sum(F.when(F.col("term").isin("2024T2", "2025T1", "2024T1"), 1).otherwise(0)).alias("n_term_in_2024t2_2025t1_2024t1"),
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.min("score").alias("min_score"),
    )
)

screened = (
    rollup_base
    .crossJoin(rollup_base.agg(F.min("cnt").alias("overall_min_cnt")))
    .filter(F.col("cnt") > F.col("overall_min_cnt"))
    .drop("overall_min_cnt")
)

filtered = (
    screened.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term") == "2025T2") & (F.col("e.enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")), "left_anti")
)

result = (
    filtered
    .select(
        "student_id",
        "enrolled_at_month",
        "cnt",
        "term_list",
        "n_term_2025t1",
        "n_term_in_2024t2_2025t1_2024t1",
        "last_enrolled_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at_month")).alias("days_since_enrolled_at_month"),
    )
    .orderBy(F.col("cnt").desc(), F.col("n_term_in_2024t2_2025t1_2024t1").desc())
)
