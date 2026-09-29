from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_quarter_grade_st = Window.partitionBy("enrolled_at_quarter", "grade_status").orderBy("avg_enrollment_score", "id")
w_grade_status = Window.partitionBy("grade_status")
w_enrolled_at_quarter_grade_st_2 = Window.partitionBy("enrolled_at_quarter", "grade_status").orderBy("avg_enrollment_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.concat_ws("|", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.round(F.avg("score"), 2).alias("avg_enrollment_score"))
)

with_counts = (
    cleaned_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.avg_enrollment_score")
)

ranked = (
    with_counts
    .select(
        "*",
        F.lag("score").over(w_enrolled_at_quarter_grade_st).alias("prev_score"),
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.round(F.col("avg_enrollment_score") / F.nullif(F.sum("avg_enrollment_score").over(w_grade_status), F.lit(0)), 4).alias("avg_enrollment_score_share_of_grade_status"),
        F.rank().over(w_enrolled_at_quarter_grade_st).alias("avg_enrollment_score_rank"),
        F.min("score").over(w_enrolled_at_quarter_grade_st_2).alias("min_score_so_far"),
        F.max("score").over(w_grade_status).alias("grade_status_max_score"),
    )
)

summary = (
    ranked
    .filter(F.col("student_id").isNotNull())
    .groupBy("grade_status")
    .agg(
        F.round(F.stddev("enrolled_at_quarter"), 2).alias("stddev_enrolled_at_quarter"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.avg("score").alias("avg_score_check"),
    )
    .filter(F.col("avg_score_check") > 34.2)
    .drop("avg_score_check")
)

with_activity = (
    summary
    .crossJoin(summary.agg(F.min("stddev_enrolled_at_quarter").alias("overall_min_stddev_enrolled_at_quarter")))
    .filter(F.col("stddev_enrolled_at_quarter") > F.col("overall_min_stddev_enrolled_at_quarter"))
    .drop("overall_min_stddev_enrolled_at_quarter")
)

with_activity_2 = (
    with_activity
    .crossJoin(with_activity.agg(F.max("stddev_enrolled_at_quarter").alias("overall_max_stddev_enrolled_at_quarter")))
    .filter(F.col("stddev_enrolled_at_quarter") < F.col("overall_max_stddev_enrolled_at_quarter"))
    .drop("overall_max_stddev_enrolled_at_quarter")
)

result = (
    with_activity_2
    .select("grade_status", "stddev_enrolled_at_quarter")
    .orderBy(F.col("stddev_enrolled_at_quarter").desc(), "grade_status")
)
