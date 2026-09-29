from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_enrolled_at_ties = Window.partitionBy("term").orderBy("enrolled_at")
w_all_by_score = Window.orderBy("score", "id")
w_student_id = Window.partitionBy("student_id")
w_all_by_score_full = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_student_enrolled_on_p = Window.orderBy("student_enrolled_on", "id").rowsBetween(-3, -1)
w_term_by_enrolled_at = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_all_by_student_enrolled_on_t = Window.orderBy("student_enrolled_on")

filtered_enrollments = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("INCOMPLETE", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "PASS", "group_b").otherwise("other").alias("grade_status_group"),
        F.concat_ws("|", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

with_details = (
    filtered_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_group",
        "s.grade_status_term_label",
        "c.course_code",
        F.col("c.level").alias("course_level"),
        "st.student_name",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

ranked = (
    with_details
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "grade_status_group",
        "grade_status_term_label",
        "course_code",
        "course_level",
        "student_name",
        F.round(F.cume_dist().over(w_term_by_enrolled_at_ties), 3).alias("enrolled_at_cume_dist"),
        F.dense_rank().over(w_term_by_enrolled_at_ties).alias("enrolled_at_dense_rank"),
        F.rank().over(w_all_by_score).alias("score_rank"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.last("student_enrolled_on").over(w_all_by_score_full).alias("last_student_enrolled_on"),
        F.round(F.avg("score").over(w_student_id), 2).alias("student_id_avg_score"),
        F.round(F.col("score") - F.avg("score").over(w_student_id), 2).alias("score_vs_student_id_avg"),
        F.round(F.avg("score").over(w_all_by_student_enrolled_on_p), 2).alias("score_avg_3_prior"),
        (F.col("score") - F.lag("score").over(w_term_by_enrolled_at)).alias("score_change"),
        F.rank().over(w_all_by_student_enrolled_on_t).alias("student_enrolled_on_rank"),
        F.lag("enrolled_at").over(w_all_by_score).alias("prev_enrolled_at"),
        F.sum("score").over(w_student_id).alias("student_id_sum_score"),
        F.size(F.collect_set("course_level").over(w_student_id)).alias("distinct_course_level_in_student_id"),
    )
)

top_rows = (
    ranked
    .filter((F.col("score_rank") == 1) & (F.col("course_id") <= 10))
    .drop("score_rank")
)

result = (
    top_rows
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrolled_at",
        "grade_status_term_label",
        "course_code",
        "course_level",
        "student_name",
        "enrolled_at_cume_dist",
        "enrolled_at_dense_rank",
        "rows_in_student_id",
        "last_student_enrolled_on",
        "student_id_avg_score",
        "score_vs_student_id_avg",
        "score_avg_3_prior",
        "student_enrolled_on_rank",
        "prev_enrolled_at",
        "student_id_sum_score",
        "distinct_course_level_in_student_id",
        (F.col("score_change") + F.col("rows_in_student_id")).alias("score_change_plus_rows_in_student_id"),
    )
    .orderBy("term", F.col("course_code").desc())
)
