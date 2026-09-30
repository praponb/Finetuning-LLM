from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter(F.col("grade_status") == "PASS")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.concat_ws(": ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

annotated = (
    valid
    .select(
        "*",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.concat_ws(" / ", F.col("term_grade_status_label"), F.col("grade_status")).alias("term_grade_status_label_grade_status_label"),
        F.lower(F.regexp_replace(F.trim(F.col("term_grade_status_label")), " ", "_")).alias("term_grade_status_label_slug"),
    )
)

result = (
    annotated
    .select(
        "id",
        "term",
        "score",
        "grade_status",
        "score_filled",
        "term_grade_status_label_slug",
    )
    .orderBy("grade_status", F.col("id").desc())
)
