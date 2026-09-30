from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_score = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id")
w_course_id = Window.partitionBy("course_id")
w_grade_status_by_score_2 = Window.partitionBy("grade_status").orderBy("score", "id")

recent_enrollments = (
    df_enrollments
    .filter((F.col("score") < 78.8) & (F.col("grade_status").isin("INCOMPLETE", "PASS")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

filtered = (
    recent_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
    .filter(F.col("term") != "2025T1")
)

windowed = (
    filtered
    .select(
        "*",
        F.round(F.cume_dist().over(w_grade_status_by_score), 3).alias("score_cume_dist"),
        F.round(F.avg("score").over(w_course_id), 2).alias("course_id_avg_score"),
        F.size(F.collect_set("term").over(w_course_id)).alias("distinct_term_in_course_id"),
        F.round(F.cume_dist().over(w_grade_status_by_score_2), 3).alias("score_cume_dist_2"),
    )
)

filtered_2 = (
    windowed.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.upper(F.trim(F.col("c.course_code"))).like("C%")) & (F.col("c.level").isin("intro", "core")), "left_anti")
)

result = (
    filtered_2
    .groupingSets(
        [["grade_status", "distinct_term_in_course_id", "course_id"], ["grade_status"], ["distinct_term_in_course_id"], []],
        "grade_status", "distinct_term_in_course_id", "course_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.avg("course_id_avg_score"), 2).alias("avg_course_id_avg_score"),
        F.countDistinct("term").alias("distinct_terms"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("distinct_term_in_course_id").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
