from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status")
w_course_id_student_id_by_scor = Window.partitionBy("course_id", "student_id").orderBy("score")
w_course_id_student_id_by_scor_2 = Window.partitionBy("course_id", "student_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_student_id_by_scor_3 = Window.partitionBy("course_id", "student_id").orderBy("score", "id")

base = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

metrics = (
    base
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "grade_status_term_label",
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.min("score").over(w_grade_status).alias("grade_status_min_score"),
        F.dense_rank().over(w_course_id_student_id_by_scor).alias("score_dense_rank"),
        F.nth_value("score", 2).over(w_course_id_student_id_by_scor_2).alias("second_score"),
        F.lead("score").over(w_course_id_student_id_by_scor_3).alias("next_score"),
        F.ntile(5).over(w_course_id_student_id_by_scor_3).alias("score_quintile"),
    )
)

filtered = (
    metrics.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
)

result = (
    filtered
    .groupingSets(
        [["grade_status", "score_dense_rank"], ["grade_status"], []],
        "grade_status", "score_dense_rank",
    )
    .agg(F.grouping_id().alias("gid"), F.count("*").alias("n_rows"))
    .select(
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "score_dense_rank",
        "gid",
        "n_rows",
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("score_dense_rank").asc_nulls_last(),
    )
)
