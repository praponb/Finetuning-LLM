from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_term = Window.partitionBy("grade_status", "term")
w_all_by_course_credits_ties = Window.orderBy(F.col("course_credits").desc())
w_all_by_course_credits_full = Window.orderBy(F.col("course_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_enrollments
    .filter(F.col("score").between(8.7, 73.0))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

joined = (
    cleaned.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        F.col("st.is_international").alias("student_is_international"),
        "st.student_name",
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
    )
)

sequenced = (
    joined
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "student_is_international",
        "student_name",
        "course_credits",
        F.sum("score").over(w_grade_status_term).alias("grade_status_term_sum_score"),
        F.round(F.percent_rank().over(w_all_by_course_credits_ties), 3).alias("course_credits_percent_rank"),
        F.last("course_credits").over(w_all_by_course_credits_full).alias("last_course_credits"),
    )
)

annotated = (
    sequenced
    .filter(F.col("student_is_international"))
    .select(
        "*",
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
        F.when(F.col("student_is_international"), "yes").otherwise("no").alias("student_is_international_label"),
    )
)

result = (
    annotated
    .cube("last_course_credits", "term", "course_credits")
    .agg(
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.round(F.avg("grade_status_term_sum_score"), 2).alias("avg_grade_status_term_sum_score"),
        F.array_sort(F.collect_set("student_name")).alias("student_name_list"),
    )
    .select(
        "last_course_credits",
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        "course_credits",
        "grade_status_list",
        "avg_grade_status_term_sum_score",
        "student_name_list",
    )
    .orderBy(
        F.col("last_course_credits").asc_nulls_last(),
        F.col("term").asc_nulls_last(),
        F.col("course_credits").asc_nulls_last(),
    )
)
