from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_dept_course_id = Window.partitionBy("course_dept", "course_id")
w_student_id_term_by_score_dis = Window.partitionBy("student_id", "term").orderBy(F.col("score_distance").desc())

filtered_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2025T2")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 4).alias("grade_status_prefix"),
        F.abs(F.col("score") - 38.3).alias("score_distance"),
    )
)

term_stats = (
    filtered_enrollments
    .groupBy("term")
    .agg(F.avg("score_distance").alias("avg_score_distance"))
)

above_average = (
    filtered_enrollments.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.score_distance") > F.col("g.avg_score_distance"))
    .select(
        "s.*",
        "g.avg_score_distance",
        F.round(F.col("s.score_distance") - F.col("g.avg_score_distance"), 2).alias("score_distance_minus_avg"),
    )
)

scored = (
    above_average
    .select(
        "*",
        F.concat_ws(" / ", F.col("grade_status_prefix"), F.col("term")).alias("grade_status_prefix_term_label"),
        F.round(F.col("score_distance_minus_avg") * 0.25, 2).alias("score_distance_minus_avg_adj"),
    )
)

with_details_course = (
    scored.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.grade_status_prefix",
        "s.score_distance",
        "s.avg_score_distance",
        "s.score_distance_minus_avg",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

with_trends = (
    with_details_course
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "enrolled_at",
        "grade_status_prefix",
        "score_distance",
        "avg_score_distance",
        "score_distance_minus_avg",
        "course_level",
        "student_gpa",
        F.round(F.col("score_distance") - F.avg("score_distance").over(w_course_dept_course_id), 2).alias("score_distance_vs_course_dept_course_id_avg"),
        F.count(F.lit(1)).over(w_course_dept_course_id).alias("rows_in_course_dept_course_id"),
        F.round(F.col("avg_score_distance") - F.avg("avg_score_distance").over(w_course_dept_course_id), 2).alias("avg_score_distance_vs_course_dept_course_id_avg"),
        F.round(F.cume_dist().over(w_student_id_term_by_score_dis), 3).alias("score_distance_cume_dist"),
    )
)

summary = (
    with_trends
    .filter(F.col("score_distance_cume_dist") < 0.7)
    .groupBy("student_id")
    .agg(
        F.sum("score_distance_cume_dist").alias("total_score_distance_cume_dist"),
        F.array_sort(F.collect_set("course_level")).alias("course_level_list"),
        F.round(F.sum("avg_score_distance"), 2).alias("total_avg_score_distance"),
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.max("score_distance_vs_course_dept_course_id_avg").alias("max_score_distance_vs_course_dept_course_id_avg_"),
    )
    .filter(F.col("max_score_distance_vs_course_dept_course_id_avg_") > -30.0)
    .drop("max_score_distance_vs_course_dept_course_id_avg_")
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.sum("score").alias("total_enrollment_score"),
        F.count("*").alias("enrollment_count"),
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
    )
)

summarized_rows = (
    summary.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.total_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.avg_enrollment_score",
    )
)

result = (
    summarized_rows
    .select(
        "student_id",
        "total_score_distance_cume_dist",
        "course_level_list",
        "last_enrolled_at",
        "total_enrollment_score",
        "enrollment_count",
        "avg_enrollment_score",
    )
    .orderBy("avg_enrollment_score")
)
