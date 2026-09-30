from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_course_level_by_score_f = Window.partitionBy("term", "course_level").orderBy("score_filled", "id")
w_course_level = Window.partitionBy("course_level")
w_term_course_level_by_score_f_2 = Window.partitionBy("term", "course_level").orderBy("score_filled")

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("term").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

tagged = cleaned_enrollments.select("*", F.length(F.trim(F.col("term"))).alias("term_len"))

enriched_student = (
    tagged.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "st.student_name",
        F.col("st.cohort").alias("student_cohort"),
        "c.course_code",
    )
)

enriched = (
    enriched_student.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.*",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
    )
)

ranked = (
    enriched
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "score_filled",
        "term_len",
        "student_name",
        "student_cohort",
        "course_code",
        "course_credits",
        "course_level",
        F.round(F.percent_rank().over(w_term_course_level_by_score_f), 3).alias("score_filled_percent_rank"),
        F.size(F.collect_set("course_dept").over(w_course_level)).alias("distinct_course_dept_in_course_level"),
        F.count(F.lit(1)).over(w_course_level).alias("rows_in_course_level"),
        F.dense_rank().over(w_term_course_level_by_score_f_2).alias("score_filled_dense_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "term",
        "score",
        "score_filled",
        "term_len",
        "student_cohort",
        "course_credits",
        "course_level",
        "score_filled_percent_rank",
        "distinct_course_dept_in_course_level",
        "rows_in_course_level",
        "score_filled_dense_rank",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled_2"),
        F.when(F.col("course_level").isin("intro", "core"), "group_a").when(F.col("course_level") == "advanced", "group_b").otherwise("other").alias("course_level_group"),
    )
    .orderBy(F.col("student_cohort").asc_nulls_last(), F.col("course_level").desc())
)
