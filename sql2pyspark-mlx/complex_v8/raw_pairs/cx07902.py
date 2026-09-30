from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_major_term = Window.partitionBy("student_major", "term")
w_grade_status_by_score_ties = Window.partitionBy("grade_status").orderBy("score")
w_enrolled_at_month_student_id = Window.partitionBy(F.month("enrolled_at"), "student_id").orderBy("enrolled_at", "id")
w_term_by_score_ties = Window.partitionBy("term").orderBy(F.col("score").desc_nulls_first())
w_term_by_score = Window.partitionBy("term").orderBy(F.col("score").desc_nulls_first(), "id")
w_term_student_id = Window.partitionBy("term", "student_id")
w_term_by_prev_enrolled_at_run = Window.partitionBy("term").orderBy("prev_enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_student_major_in_te = Window.partitionBy("distinct_student_major_in_term_student_id", "term").orderBy(F.col("student_enrolled_on").asc_nulls_last(), "id")
w_distinct_student_major_in_te_2 = Window.partitionBy("distinct_student_major_in_term_student_id").orderBy("distinct_student_major_in_term_student_id", "id")

scoped = (
    df_enrollments
    .filter((F.col("score") > 38.3) & (F.col("course_id") <= 10))
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

with_parent_student = (
    scoped.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.enrolled_on").between(F.lit("2024-03-15").cast("date"), F.lit("2025-06-27").cast("date")))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.major").alias("student_major"),
    )
)

with_trends = (
    with_parent_student
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "student_enrolled_on",
        F.count(F.lit(1)).over(w_student_major_term).alias("rows_in_student_major_term"),
        F.round(F.cume_dist().over(w_grade_status_by_score_ties), 3).alias("score_cume_dist"),
        F.lead("student_enrolled_on").over(w_enrolled_at_month_student_id).alias("next_student_enrolled_on"),
        F.dense_rank().over(w_term_by_score_ties).alias("score_dense_rank"),
        F.lag("enrolled_at").over(w_term_by_score).alias("prev_enrolled_at"),
        F.size(F.collect_set("course_id").over(w_term_student_id)).alias("distinct_course_id_in_term_student_id"),
        F.round(F.col("score") - F.avg("score").over(w_student_major_term), 2).alias("score_vs_student_major_term_avg"),
        F.lag("student_enrolled_on", 3).over(w_enrolled_at_month_student_id).alias("prev3_student_enrolled_on"),
        F.size(F.collect_set("student_major").over(w_term_student_id)).alias("distinct_student_major_in_term_student_id"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "*",
        F.min("score_cume_dist").over(w_term_by_prev_enrolled_at_run).alias("min_score_cume_dist_so_far"),
        F.ntile(10).over(w_distinct_student_major_in_te).alias("student_enrolled_on_decile"),
        F.row_number().over(w_distinct_student_major_in_te_2).alias("rn"),
    )
)

result = (
    with_trends_2
    .filter(F.col("score_cume_dist") > 0.3)
    .select(
        "id",
        "course_id",
        "student_enrolled_on",
        "rows_in_student_major_term",
        "score_cume_dist",
        "score_dense_rank",
        "prev_enrolled_at",
        "distinct_course_id_in_term_student_id",
        "score_vs_student_major_term_avg",
        "prev3_student_enrolled_on",
        "distinct_student_major_in_term_student_id",
        "min_score_cume_dist_so_far",
        "student_enrolled_on_decile",
        "rn",
        F.when(F.col("term").isin("2024T1", "2025T1"), "group_a").when(F.col("term") == "2024T2", "group_b").otherwise("other").alias("term_group"),
        F.round((F.unix_timestamp(F.col("prev_enrolled_at")) - F.unix_timestamp(F.col("enrolled_at"))) / 3600, 1).alias("hours_enrolled_at_to_prev_enrolled_at"),
    )
    .orderBy(
        F.col("student_enrolled_on").desc(),
        F.col("rows_in_student_major_term").desc(),
    )
)
