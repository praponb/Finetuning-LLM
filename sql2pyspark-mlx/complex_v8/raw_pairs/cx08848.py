from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("student_id", "course_id").orderBy(F.col("score_distance").desc(), F.col("id").desc())
w_term_by_score_ties = Window.partitionBy("term").orderBy(F.col("score").desc())
w_term_student_id = Window.partitionBy("term", "student_id")

recent = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.hour(F.col("enrolled_at")).between(8, 12)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.abs(F.col("score") - 32.9).alias("score_distance"),
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.count("*").alias("enrollment_count"))
)

with_counts = (
    recent.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

deduped = (
    with_counts
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

combined_details_course = (
    deduped.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.*",
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
    )
)

with_parent_course = (
    combined_details_course.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.enrollment_count",
        "s.course_dept",
        "s.course_code",
        F.col("c.course_code").alias("course_course_code"),
        F.col("c.level").alias("course_level"),
    )
)

with_trends = (
    with_parent_course
    .select(
        "*",
        F.round(F.cume_dist().over(w_term_by_score_ties), 3).alias("score_cume_dist"),
        F.sum("enrollment_count").over(w_term_student_id).alias("term_student_id_sum_enrollment_count"),
        F.count(F.lit(1)).over(w_term_student_id).alias("rows_in_term_student_id"),
        F.round(F.col("enrollment_count") / F.nullif(F.sum("enrollment_count").over(w_term_student_id), F.lit(0)), 4).alias("enrollment_count_share_of_term_student_id"),
        F.round(F.col("enrollment_count") - F.avg("enrollment_count").over(w_term_student_id), 2).alias("enrollment_count_vs_term_student_id_avg"),
        F.round(F.col("score") - F.avg("score").over(w_term_student_id), 2).alias("score_vs_term_student_id_avg"),
    )
)

result = (
    with_trends
    .filter(F.lower(F.col("course_code")).like("%10%"))
    .select(
        "id",
        "term",
        "score",
        "enrolled_at",
        "course_dept",
        "course_code",
        "course_course_code",
        "score_cume_dist",
        "enrollment_count_share_of_term_student_id",
        "score_vs_term_student_id_avg",
        F.concat(F.col("course_dept"), F.lit("#"), F.col("id").cast("string")).alias("course_dept_ref"),
    )
    .orderBy(F.col("score_vs_term_student_id_avg").asc_nulls_last(), "id")
    .limit(20)
)
