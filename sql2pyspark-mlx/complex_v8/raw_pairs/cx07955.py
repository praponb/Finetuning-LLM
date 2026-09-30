from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_term_len_ties = Window.partitionBy("student_id").orderBy("term_len")
w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_all_by_enrolled_at_running = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_running_2 = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_term_len_movin = Window.partitionBy("student_id").orderBy("term_len", "id").rowsBetween(-2, Window.currentRow)

recent = (
    df_enrollments
    .filter((F.col("student_id") >= 14) & (F.col("course_id") >= 2))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

labeled = (
    recent
    .select(
        "*",
        F.substring(F.trim(F.col("grade_status")), 1, 2).alias("grade_status_prefix"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
    )
)

without_matches = (
    labeled.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits") > 2), "left_semi")
)

metrics = (
    without_matches
    .select(
        "*",
        F.rank().over(w_student_id_by_term_len_ties).alias("term_len_rank"),
        F.max("score").over(w_grade_status_course_id).alias("grade_status_course_id_max_score"),
        F.min("term_len").over(w_all_by_enrolled_at_running).alias("min_term_len_so_far"),
        F.last("enrolled_at", ignorenulls=True).over(w_all_by_enrolled_at_running_2).alias("last_known_enrolled_at"),
        F.round(F.avg("score").over(w_student_id_by_term_len_movin), 2).alias("score_avg_3"),
    )
)

result = (
    metrics
    .rollup("student_id", "grade_status")
    .agg(F.round(F.stddev("term_len"), 2).alias("stddev_term_len"))
    .select(
        "student_id",
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "stddev_term_len",
    )
)
