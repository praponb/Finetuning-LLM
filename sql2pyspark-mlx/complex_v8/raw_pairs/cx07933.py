from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_grade_status = Window.partitionBy("course_id", "grade_status")
w_term_student_id_by_score_buc = Window.partitionBy("term", "student_id").orderBy(F.col("score_bucket").desc(), "id").rowsBetween(-6, Window.currentRow)
w_grade_status_student_id_by_a = Window.partitionBy("grade_status", "student_id").orderBy(F.col("avg_score").desc(), "id")

scoped_enrollments = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.substring(F.trim(F.col("term")), 1, 3).alias("term_prefix"),
    )
)

student_id_stats = (
    scoped_enrollments
    .groupBy("student_id")
    .agg(F.avg("score").alias("avg_score"))
)

vs_group = (
    scoped_enrollments.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

with_windows = (
    vs_group
    .select(
        "*",
        F.round(F.col("score") - F.avg("score").over(w_course_id_grade_status), 2).alias("score_vs_course_id_grade_status_avg"),
        F.round(F.avg("score_bucket").over(w_term_student_id_by_score_buc), 2).alias("score_bucket_avg_7"),
        F.rank().over(w_grade_status_student_id_by_a).alias("avg_score_rank"),
        F.size(F.collect_set("term").over(w_course_id_grade_status)).alias("distinct_term_in_course_id_grade_status"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "score_bucket",
        "term_prefix",
        "avg_score",
        "score_minus_avg",
        F.coalesce(F.col("score_bucket"), F.lit(0)).alias("score_bucket_filled"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
    .orderBy(F.col("id").desc())
)
