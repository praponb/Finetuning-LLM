from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id")
w_student_id_by_score_distance = Window.partitionBy("student_id").orderBy("score_distance", "id").rowsBetween(-2, -1)
w_enrollment_count_by_enrolled = Window.partitionBy("enrollment_count").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_grade_status = Window.partitionBy("term", "grade_status")

valid = (
    df_enrollments
    .filter(F.col("score") < 81.4)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 43.5).alias("score_distance"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.count("*").alias("enrollment_count"))
)

profiled = (
    valid.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

qualified = (
    profiled
    .crossJoin(profiled.agg(F.min("enrollment_count").alias("overall_min_enrollment_count")))
    .filter(F.col("enrollment_count") > F.col("overall_min_enrollment_count"))
    .drop("overall_min_enrollment_count")
)

sequenced = (
    qualified
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
        F.round(F.avg("enrollment_count").over(w_student_id_by_score_distance), 2).alias("enrollment_count_avg_2_prior"),
        F.last("score_distance", ignorenulls=True).over(w_enrollment_count_by_enrolled).alias("last_known_score_distance"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
    )
)

student_id_stats = (
    sequenced
    .groupBy("student_id")
    .agg(F.avg("score").alias("avg_score"))
)

above_average = (
    sequenced.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

with_trends = (
    above_average
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "last_known_score_distance",
        "rows_in_course_id",
        "avg_score",
        "score_minus_avg",
        F.size(F.collect_set("student_id").over(w_term_grade_status)).alias("distinct_student_id_in_term_grade_status"),
        F.round(F.avg("score").over(w_term_grade_status), 2).alias("term_grade_status_avg_score"),
        F.round(F.col("score_share_of_course_id") / F.nullif(F.sum("score_share_of_course_id").over(w_term_grade_status), F.lit(0)), 4).alias("score_share_of_course_id_share_of_term_grade_sta"),
        F.max("avg_score").over(w_term_grade_status).alias("term_grade_status_max_avg_score"),
        F.count(F.lit(1)).over(w_term_grade_status).alias("rows_in_term_grade_status"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "last_known_score_distance",
        "rows_in_course_id",
        "distinct_student_id_in_term_grade_status",
        "term_grade_status_avg_score",
        "score_share_of_course_id_share_of_term_grade_sta",
        "term_grade_status_max_avg_score",
        "rows_in_term_grade_status",
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
