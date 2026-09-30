from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_grade_status_group = Window.partitionBy("course_id", "grade_status_group")
w_student_id_by_score = Window.partitionBy("student_id").orderBy("score", "id")
w_student_id_by_score_running = Window.partitionBy("student_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("course_id") <= 6) & (~F.col("grade_status").isin("FAIL", "WITHDRAWN")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("WITHDRAWN", "INCOMPLETE"), "group_a").when(F.col("grade_status") == "PASS", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

with_trends = (
    cleaned_enrollments
    .select(
        "*",
        F.count(F.lit(1)).over(w_course_id_grade_status_group).alias("rows_in_course_id_grade_status_group"),
        F.lag("enrolled_at", 2).over(w_student_id_by_score).alias("prev2_enrolled_at"),
        F.sum("score").over(w_student_id_by_score_running).alias("running_score"),
    )
)

term_stats = (
    with_trends
    .groupBy("term")
    .agg(F.avg("running_score").alias("avg_running_score"))
)

outliers = (
    with_trends.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.running_score") > F.col("g.avg_running_score"))
    .select(
        "s.*",
        "g.avg_running_score",
        F.round(F.col("s.running_score") - F.col("g.avg_running_score"), 2).alias("running_score_minus_avg"),
    )
)

screened = (
    outliers.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
    .filter(F.col("term") != "2025T1")
)

result = (
    screened
    .cube("course_id", "grade_status_group")
    .agg(
        F.grouping_id().alias("gid"),
        F.max("running_score_minus_avg").alias("max_running_score_minus_avg"),
    )
    .select(
        "course_id",
        F.coalesce(F.col("grade_status_group"), F.lit("ALL")).alias("grade_status_group"),
        "gid",
        "max_running_score_minus_avg",
    )
)
