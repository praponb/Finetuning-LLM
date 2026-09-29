from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_prior_6 = Window.orderBy(F.col("score").asc_nulls_last(), "id", "enrollment_id").rowsBetween(-6, -1)
w_all_by_score_ties = Window.orderBy(F.col("score").asc_nulls_last())

valid_enrollments = (
    df_enrollments
    .filter((F.col("score") < 83.1) & (F.col("course_id") <= 9))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.round(F.col("score") * 0.9, 2).alias("score_adj"),
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
    )
)

line_level = (
    valid_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.grade_status_prefix",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
    )
)

with_trends = (
    line_level
    .withColumn("score_avg_6_prior", F.round(F.avg("score").over(w_all_by_score_prior_6), 2))
    .withColumn("score_rank", F.rank().over(w_all_by_score_ties))
)

without_matches = (
    with_trends.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.length(F.trim(F.col("st.student_name"))) >= 8) & (F.col("st.cohort") == "C2022"), "left_semi")
)

result = (
    without_matches
    .groupingSets(
        [["student_id", "term"], ["student_id"]],
        "student_id", "term",
    )
    .agg(
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
    )
)
