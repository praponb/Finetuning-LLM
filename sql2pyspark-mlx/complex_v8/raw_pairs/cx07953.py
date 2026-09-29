from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_score_ties = Window.partitionBy("term").orderBy("score")
w_term_by_score_moving_6 = Window.partitionBy("term").orderBy("score", "id").rowsBetween(-6, Window.currentRow)
w_course_id_student_id = Window.partitionBy("course_id", "student_id")

filtered_enrollments = (
    df_enrollments
    .filter(F.col("student_id") <= 14)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.when(F.col("score") >= 62.5, "A").when(F.col("score") >= 11.3, "B").otherwise("C").alias("score_tier"),
    )
)

with_windows = (
    filtered_enrollments
    .select(
        "*",
        F.round(F.cume_dist().over(w_term_by_score_ties), 3).alias("score_cume_dist"),
        F.round(F.avg("score").over(w_term_by_score_moving_6), 2).alias("score_avg_7"),
        F.size(F.collect_set("term").over(w_course_id_student_id)).alias("distinct_term_in_course_id_student_id"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id_student_id), F.lit(0)), 4).alias("score_share_of_course_id_student_id"),
    )
)

without_matches = (
    with_windows.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id").isNotNull()), "left_anti")
)

result = (
    without_matches
    .select("id", "score_share_of_course_id_student_id", "score_cume_dist")
    .unpivot(
        ids=["id"],
        values=["score_share_of_course_id_student_id", "score_cume_dist"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
