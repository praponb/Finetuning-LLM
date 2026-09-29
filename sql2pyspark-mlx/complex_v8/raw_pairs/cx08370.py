from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_grade_status_or_u = Window.partitionBy("student_id", "grade_status_or_unknown").orderBy(F.col("score").desc(), "id")
w_term = Window.partitionBy("term")
w_score_dense_rank_score_seq_b = Window.partitionBy("score_dense_rank", "score_seq").orderBy("score_rank")
w_course_id_by_score_filled_mo = Window.partitionBy("course_id").orderBy(F.col("score_filled").desc(), "id").rowsBetween(-6, Window.currentRow)

cleaned_enrollments = (
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
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_trends = (
    cleaned_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "grade_status_or_unknown",
        "term_slug",
        "score_filled",
        F.dense_rank().over(w_student_id_grade_status_or_u).alias("score_dense_rank"),
        F.row_number().over(w_student_id_grade_status_or_u).alias("score_seq"),
        F.rank().over(w_student_id_grade_status_or_u).alias("score_rank"),
        F.sum("score").over(w_term).alias("term_sum_score"),
    )
)

with_windows = (
    with_trends
    .select(
        "id",
        "course_id",
        "enrolled_at",
        "grade_status_or_unknown",
        "term_slug",
        "score_filled",
        "score_dense_rank",
        "score_seq",
        "term_sum_score",
        F.rank().over(w_score_dense_rank_score_seq_b).alias("score_rank_rank"),
        F.round(F.avg("score_seq").over(w_course_id_by_score_filled_mo), 2).alias("score_seq_avg_7"),
    )
)

tagged = (
    with_windows
    .select(
        "*",
        F.coalesce(F.col("score_filled"), F.lit(0.0)).alias("score_filled_filled"),
    )
)

with_activity = (
    tagged.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status").isNotNull()), "left_anti")
)

result = (
    with_activity
    .select(
        "id",
        "enrolled_at",
        "grade_status_or_unknown",
        "term_slug",
        "score_filled",
        "term_sum_score",
        "score_rank_rank",
        "score_filled_filled",
    )
    .orderBy(
        F.col("term_sum_score").asc_nulls_last(),
        F.col("score_filled_filled").desc(),
    )
)
