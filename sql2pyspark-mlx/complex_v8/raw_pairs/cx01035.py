from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_month_course_id = Window.partitionBy(F.month("enrolled_at"), "course_id")
w_student_id_course_id_by_scor = Window.partitionBy("student_id", "course_id").orderBy(F.col("score").desc())
w_latest = Window.partitionBy("grade_status", "term").orderBy(F.col("score_cume_dist").desc(), F.col("id").desc())

prepared_enrollments = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.substring(F.trim(F.col("grade_status")), 1, 2).alias("grade_status_prefix"),
    )
)

with_trends = (
    prepared_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "score_filled",
        "grade_status_prefix",
        F.round(F.col("score_filled") / F.nullif(F.sum("score_filled").over(w_enrolled_at_month_course_id), F.lit(0)), 4).alias("score_filled_share_of_enrolled_at_month_course_i"),
        F.round(F.cume_dist().over(w_student_id_course_id_by_scor), 3).alias("score_cume_dist"),
    )
)

qualified = (
    with_trends.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") >= 35.8), "left_semi")
    .filter(F.col("grade_status_prefix").isNotNull())
)

latest_per_key = (
    qualified
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select("id", "score_cume_dist", "score_filled")
    .unpivot(
        ids=["id"],
        values=["score_cume_dist", "score_filled"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
