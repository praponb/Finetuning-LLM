from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter(F.col("score") < 81.4)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

labeled = (
    prepared
    .select(
        "id",
        "student_id",
        "grade_status",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

screened = (
    labeled
    .crossJoin(labeled.agg(F.avg("score_bucket").alias("overall_avg_score_bucket")))
    .filter(F.col("score_bucket") > F.col("overall_avg_score_bucket"))
    .drop("overall_avg_score_bucket")
)

result = (
    screened
    .groupBy("student_id")
    .pivot("grade_status", ["PASS", "WITHDRAWN"])
    .agg(F.sum("score_bucket"))
    .orderBy("student_id")
)
