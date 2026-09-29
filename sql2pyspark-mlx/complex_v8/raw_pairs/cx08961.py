from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_session_no = Window.partitionBy("grade_status", "session_no")
w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").desc())
w_grade_status_session_no_by_p = Window.partitionBy("grade_status", "session_no").orderBy("prev_enrolled_at", "id")
w_session_no_by_session_no = Window.partitionBy("session_no").orderBy("session_no", "id")

filtered = (
    df_enrollments
    .filter(F.col("grade_status") == "INCOMPLETE")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
    )
)

with_prev = filtered.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_term_running))

windowed = (
    sessionized
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "is_new_session",
        "session_no",
        F.round(F.col("session_no") - F.avg("session_no").over(w_grade_status_session_no), 2).alias("session_no_vs_grade_status_session_no_avg"),
        F.dense_rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_dense_rank"),
        F.round(F.cume_dist().over(w_grade_status_session_no_by_p), 3).alias("prev_enrolled_at_cume_dist"),
        F.row_number().over(w_session_no_by_session_no).alias("rn"),
    )
)

result = (
    windowed
    .groupingSets(
        [["student_id", "is_new_session", "course_id"], ["student_id"], ["is_new_session"], []],
        "student_id", "is_new_session", "course_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.percentile_approx("score", 0.5).alias("median_score"),
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("is_new_session").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
