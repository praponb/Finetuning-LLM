from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter((F.col("score") > 13.9) & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

enriched = (
    recent
    .filter(F.col("grade_status").isNotNull())
    .select(
        "*",
        F.concat_ws(": ", F.col("grade_status_clean"), F.col("term")).alias("grade_status_clean_term_label"),
    )
)

result = (
    enriched
    .groupBy("course_id")
    .pivot("grade_status", ["INCOMPLETE", "PASS", "WITHDRAWN"])
    .agg(F.avg("score").alias("avg_v"), F.count("score").alias("n"))
    .orderBy("course_id")
)
