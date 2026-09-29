from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_month_course_id = Window.partitionBy(F.month("enrolled_at"), "course_id")
w_course_id_student_id_by_term = Window.partitionBy("course_id", "student_id").orderBy(F.col("term_len").desc(), "id")

cleaned = (
    df_enrollments
    .filter((F.col("grade_status") == "INCOMPLETE") | (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

with_trends = (
    cleaned
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "term_len",
        F.size(F.collect_set("grade_status").over(w_enrolled_at_month_course_id)).alias("distinct_grade_status_in_enrolled_at_month_cours"),
        F.row_number().over(w_course_id_student_id_by_term).alias("seq_no"),
        F.min("term_len").over(w_enrolled_at_month_course_id).alias("enrolled_at_month_course_id_min_term_len"),
        F.lag("term_len").over(w_course_id_student_id_by_term).alias("prev_term_len"),
        F.round(F.col("term_len") / F.nullif(F.sum("term_len").over(w_enrolled_at_month_course_id), F.lit(0)), 4).alias("term_len_share_of_enrolled_at_month_course_id"),
    )
)

result = (
    with_trends
    .cube("distinct_grade_status_in_enrolled_at_month_cours", "prev_term_len")
    .agg(
        F.count("*").alias("cnt"),
        F.round(100 * F.count(F.when(F.col("grade_status").isin("PASS", "WITHDRAWN", "INCOMPLETE"), 1)) / F.count("*"), 1).alias("pct_grade_status_in_pass_withdra"),
    )
    .orderBy(
        F.col("distinct_grade_status_in_enrolled_at_month_cours").asc_nulls_last(),
        F.col("prev_term_len").asc_nulls_last(),
    )
)
