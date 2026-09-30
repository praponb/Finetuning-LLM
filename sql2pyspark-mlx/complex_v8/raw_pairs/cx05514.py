from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("term").orderBy("score_bucket", "id")
w_term_distinct_grade_status_b = Window.partitionBy("term", "distinct_grade_status").orderBy(F.col("distinct_grade_status").desc(), "term").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_grade_status = Window.partitionBy("distinct_grade_status")

base_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 10)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

with_parent_course = (
    base_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.level") == "advanced"), "inner")
    .select("s.*", F.col("c.dept").alias("course_dept"))
)

with_details_course = (
    with_parent_course.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.score_filled",
        "s.score_bucket",
        "s.course_dept",
        F.col("c.level").alias("course_level"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

deduped = (
    with_details_course
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

rollup_base = (
    deduped
    .groupBy("term")
    .agg(
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.sum("score_bucket").alias("total_score_bucket"),
        F.min("score_bucket").alias("min_score_bucket"),
        F.count("*").alias("cnt"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
    )
    .filter(F.col("cnt") >= 2)
)

ranked = (
    rollup_base
    .select(
        "*",
        F.nth_value("distinct_grade_status", 2).over(w_term_distinct_grade_status_b).alias("second_distinct_grade_status"),
        F.min("distinct_grade_status").over(w_distinct_grade_status).alias("distinct_grade_status_min_distinct_grade_status"),
        F.size(F.collect_set("term").over(w_distinct_grade_status)).alias("distinct_term_in_distinct_grade_status"),
        F.first("distinct_grade_status").over(w_term_distinct_grade_status_b).alias("first_distinct_grade_status"),
    )
)

result = (
    ranked
    .select(
        "term",
        "term_list",
        "total_score_bucket",
        "cnt",
        "distinct_grade_status",
        "distinct_grade_status_min_distinct_grade_status",
        "distinct_term_in_distinct_grade_status",
        "first_distinct_grade_status",
    )
    .orderBy("distinct_term_in_distinct_grade_status", F.col("cnt").desc())
)
