from pyspark.sql import functions as F

in_window = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.score").alias("prior_score_12h"),
    )
)

combined_details_student = (
    in_window.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        F.col("st.cohort").alias("student_cohort"),
    )
)

grouped = (
    combined_details_student
    .groupBy("student_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    grouped
    .select(
        "student_id",
        "distinct_ids",
        "n_rows",
        F.least(F.col("n_rows"), F.col("distinct_ids")).alias("least_n_rows_distinct_ids"),
        F.least(F.col("distinct_ids"), F.col("n_rows")).alias("least_distinct_ids_n_rows"),
    )
    .orderBy(F.col("n_rows").desc_nulls_first())
)
