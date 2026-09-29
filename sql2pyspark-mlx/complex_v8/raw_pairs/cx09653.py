from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_grade_status_by_en = Window.partitionBy("course_id", "grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_or_unknown_student_id = Window.partitionBy("term_or_unknown", "student_id")

filtered = (
    df_enrollments
    .filter(F.col("score") >= 34.2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

with_trends = (
    filtered
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "grade_status_len",
        "term_or_unknown",
        F.first("score").over(w_course_id_grade_status_by_en).alias("first_score"),
        F.round(F.col("grade_status_len") / F.nullif(F.sum("grade_status_len").over(w_term_or_unknown_student_id), F.lit(0)), 4).alias("grade_status_len_share_of_term_or_unknown_studen"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "first_score",
        "grade_status_len_share_of_term_or_unknown_studen",
        "score",
    )
    .unpivot(
        ids=["id"],
        values=["first_score", "grade_status_len_share_of_term_or_unknown_studen", "score"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .orderBy("id", "metric_name")
)
