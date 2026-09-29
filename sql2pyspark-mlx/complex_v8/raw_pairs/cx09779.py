from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter((F.col("score").between(5.6, 63.9)) & (F.col("grade_status").isin("FAIL", "WITHDRAWN")))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.when(F.col("grade_status").isin("FAIL", "PASS"), "group_a").when(F.col("grade_status") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

derived = (
    prepared
    .filter(F.col("grade_status").isin("WITHDRAWN", "FAIL", "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "enrolled_at",
        "grade_status_group",
        F.when(F.col("grade_status_group").isin("group_a", "other"), "group_a").when(F.col("grade_status_group") == "group_b", "group_b").otherwise("other").alias("grade_status_group_group"),
        F.concat_ws(" / ", F.col("grade_status"), F.col("grade_status_group")).alias("grade_status_grade_status_group_label"),
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
    )
)

screened = (
    derived.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.grade_status") == "INCOMPLETE") & (F.col("e.term").isin("2025T2", "2025T1")), "left_anti")
)

result = (
    screened
    .filter(F.col("enrolled_at") < F.lit("2025-02-24 10:00:00").cast("timestamp"))
    .select(
        "id",
        "score",
        "enrolled_at",
        "grade_status_group_group",
    )
    .orderBy(F.col("grade_status_group_group").desc(), "id")
    .limit(20)
)
