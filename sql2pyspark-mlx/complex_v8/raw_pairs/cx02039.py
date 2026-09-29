from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_course_id_by_score = Window.partitionBy("term", "course_id").orderBy(F.col("score").desc(), "id")
w_student_id = Window.partitionBy("student_id")
w_score_tercile_grade_status_l = Window.partitionBy("score_tercile", "grade_status_len")
w_term_by_grade_status_len = Window.partitionBy("term").orderBy(F.col("grade_status_len").desc(), "id")

filtered_enrollments = (
    df_enrollments
    .filter((F.col("course_id") <= 4) | (F.col("score").between(5.6, 59.8)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

sequenced = (
    filtered_enrollments
    .select(
        "id",
        "term",
        "score",
        "grade_status",
        "term_ref",
        "grade_status_len",
        F.ntile(3).over(w_term_course_id_by_score).alias("score_tercile"),
        F.sum("score").over(w_student_id).alias("student_id_sum_score"),
    )
)

filtered = (
    sequenced
    .crossJoin(sequenced.agg(F.min("score").alias("overall_min_score")))
    .filter(F.col("score") > F.col("overall_min_score"))
    .drop("overall_min_score")
)

metrics = (
    filtered
    .select(
        "id",
        "term",
        "term_ref",
        "grade_status_len",
        "student_id_sum_score",
        F.round(F.col("grade_status_len") - F.avg("grade_status_len").over(w_score_tercile_grade_status_l), 2).alias("grade_status_len_vs_score_tercile_grade_status_l"),
        F.sum("grade_status_len").over(w_score_tercile_grade_status_l).alias("score_tercile_grade_status_len_sum_grade_status_"),
        F.max("student_id_sum_score").over(w_score_tercile_grade_status_l).alias("score_tercile_grade_status_len_max_student_id_su"),
        F.round(F.col("student_id_sum_score") - F.avg("student_id_sum_score").over(w_score_tercile_grade_status_l), 2).alias("student_id_sum_score_vs_score_tercile_grade_stat"),
        F.size(F.collect_set("term").over(w_score_tercile_grade_status_l)).alias("distinct_term_in_score_tercile_grade_status_len"),
        F.lead("score_tercile", 1, 0).over(w_term_by_grade_status_len).alias("next_score_tercile"),
    )
)

result = (
    metrics
    .select(
        "id",
        "term",
        "term_ref",
        "grade_status_len",
        "student_id_sum_score",
        "score_tercile_grade_status_len_max_student_id_su",
        "student_id_sum_score_vs_score_tercile_grade_stat",
        "distinct_term_in_score_tercile_grade_status_len",
        "next_score_tercile",
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref_2"),
    )
    .orderBy("term_ref_2")
)
