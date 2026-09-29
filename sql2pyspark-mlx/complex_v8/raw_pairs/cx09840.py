from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id")
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_distinct_term_in_student_id = Window.partitionBy("distinct_term_in_student_id", "score_quintile")
w_all_by_enrolled_at_running = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_enrollments
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

windowed = (
    recent
    .withColumn("distinct_term_in_student_id", F.size(F.collect_set("term").over(w_student_id)))
    .withColumn("score_quintile", F.ntile(5).over(w_all_by_score))
)

sequenced = (
    windowed
    .select(
        "id",
        "student_id",
        "score",
        "distinct_term_in_student_id",
        "score_quintile",
        F.row_number().over(w_all_by_enrolled_at).alias("seq_no"),
        F.round(F.col("score") - F.avg("score").over(w_distinct_term_in_student_id), 2).alias("score_vs_distinct_term_in_student_id_score_quint"),
        F.sum("score").over(w_all_by_enrolled_at_running).alias("running_score"),
        F.lag("distinct_term_in_student_id").over(w_all_by_enrolled_at).alias("prev_distinct_term_in_student_id"),
        F.size(F.collect_set("grade_status").over(w_distinct_term_in_student_id)).alias("distinct_grade_status_in_distinct_term_in_studen"),
    )
)

result = (
    sequenced
    .cube("student_id", "score_quintile", "prev_distinct_term_in_student_id")
    .agg(
        F.sum("prev_distinct_term_in_student_id").alias("total_prev_distinct_term_in_student_id"),
    )
)
