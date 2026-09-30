from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy(F.col("score").asc_nulls_last(), "id")
w_all_by_score_running = Window.orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id = Window.partitionBy("student_id")

base_enrollments = (
    df_enrollments
    .filter(F.col("grade_status") != "INCOMPLETE")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrolled_at_plus_30m"),
    )
)

filtered = (
    base_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.level") == "core"), "left_semi")
)

with_trends = (
    filtered
    .select(
        "*",
        F.ntile(10).over(w_all_by_score).alias("score_decile"),
        F.max("score").over(w_all_by_score_running).alias("max_score_so_far"),
        F.size(F.collect_set("term").over(w_student_id)).alias("distinct_term_in_student_id"),
        F.lead("enrolled_at", 3).over(w_all_by_score).alias("next3_enrolled_at"),
        F.sum("score").over(w_student_id).alias("student_id_sum_score"),
        F.max("score").over(w_student_id).alias("student_id_max_score"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.sum("score").alias("total_enrollment_score"),
    )
)

profiled = (
    with_trends.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.min_enrollment_score", "e.total_enrollment_score")
)

tagged = (
    profiled
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "enrolled_at_plus_30m",
        "score_decile",
        "next3_enrolled_at",
        "student_id_sum_score",
        "student_id_max_score",
        "min_enrollment_score",
        "total_enrollment_score",
        F.abs(F.col("total_enrollment_score") - 180.0).alias("total_enrollment_score_distance"),
    )
)

term_stats = (
    tagged
    .groupBy("term")
    .agg(F.percentile_approx("student_id_sum_score", 0.5).alias("median_student_id_sum_score"))
)

vs_group = (
    tagged.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.student_id_sum_score") >= F.col("g.median_student_id_sum_score"))
    .select(
        "s.*",
        "g.median_student_id_sum_score",
        F.round(F.col("s.student_id_sum_score") - F.col("g.median_student_id_sum_score"), 2).alias("student_id_sum_score_minus_median"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "student_id_sum_score_minus_median",
        "student_id_max_score",
        "total_enrollment_score_distance",
    )
    .unpivot(
        ids=["id"],
        values=["student_id_sum_score_minus_median", "student_id_max_score", "total_enrollment_score_distance"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
