from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_course_id_grade_status = Window.partitionBy("course_id", "grade_status")
w_course_id_by_score = Window.partitionBy("course_id").orderBy("score", "id")
w_course_id_by_score_ties = Window.partitionBy("course_id").orderBy("score")
w_all_by_enrolled_at_running = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_student_id = Window.partitionBy("term", "student_id")
w_term_grade_status_by_enrolle = Window.partitionBy("term", "grade_status").orderBy("enrolled_at")

base_enrollments = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("enrolled_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
    )
)

metrics = (
    base_enrollments
    .select(
        "*",
        F.dense_rank().over(w_all_by_enrolled_at).alias("enrolled_at_dense_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id_grade_status), F.lit(0)), 4).alias("score_share_of_course_id_grade_status"),
        (F.col("score") - F.lag("score").over(w_course_id_by_score)).alias("score_change"),
        F.rank().over(w_course_id_by_score_ties).alias("score_rank"),
        F.count(F.lit(1)).over(w_all_by_enrolled_at_running).alias("running_count"),
        F.round(F.avg("score").over(w_term_student_id), 2).alias("term_student_id_avg_score"),
        F.round(F.cume_dist().over(w_term_grade_status_by_enrolle), 3).alias("enrolled_at_cume_dist"),
        F.max("score").over(w_course_id_grade_status).alias("course_id_grade_status_max_score"),
    )
)

annotated = (
    metrics
    .filter(F.col("term") == "2025T1")
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "grade_status_ref",
        "enrolled_at_dense_rank",
        "score_share_of_course_id_grade_status",
        "score_change",
        "running_count",
        "term_student_id_avg_score",
        "enrolled_at_cume_dist",
        "course_id_grade_status_max_score",
        F.lower(F.trim(F.col("term"))).alias("term_clean"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        (F.col("running_count") - F.col("enrolled_at_dense_rank")).alias("running_count_minus_enrolled_at_dense_rank"),
    )
)

result = (
    annotated
    .select(
        "id",
        "enrolled_at",
        "grade_status_ref",
        "enrolled_at_dense_rank",
        "score_share_of_course_id_grade_status",
        "score_change",
        "running_count",
        "term_student_id_avg_score",
        "course_id_grade_status_max_score",
        "term_clean",
        "running_count_minus_enrolled_at_dense_rank",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
    )
    .orderBy(F.col("score_share_of_course_id_grade_status").desc())
)
