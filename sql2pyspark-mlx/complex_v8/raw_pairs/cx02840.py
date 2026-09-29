from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrollment_count_term_by_sco = Window.partitionBy("enrollment_count", "term").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_enrollment_term = Window.partitionBy("grade_status", "enrollment_term_count")
w_grade_status_enrollment_coun = Window.partitionBy("grade_status", "enrollment_count")
w_enrollment_count_last_enroll = Window.partitionBy("enrollment_count", "last_enrollment_term_count").orderBy(F.col("first_enrollment_enrolled_at").desc(), "id")
w_enrollment_count_last_enroll_2 = Window.partitionBy("enrollment_count", "last_enrollment_term_count").orderBy(F.col("first_enrollment_enrolled_at").desc(), "id").rowsBetween(-1, -1)
w_enrollment_count_last_enroll_3 = Window.partitionBy("enrollment_count", "last_enrollment_term_count").orderBy(F.col("first_enrollment_enrolled_at").desc())
w_latest = Window.partitionBy("term").orderBy(F.col("first_enrollment_enrolled_at_cume_dist").desc(), F.col("id").desc())

base = (
    df_enrollments
    .filter((F.to_date(F.col("enrolled_at")) < F.lit("2025-02-24").cast("date")) | (F.col("score") < 69.6))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.abs(F.col("score") - 34.2).alias("score_distance"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count(F.when(F.col("term") == "2024T2", 1)).alias("enrollment_term_count"),
    )
)

with_stats = (
    base.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

profiled = (
    with_stats.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.first_enrollment_enrolled_at", "e.last_enrollment_enrolled_at")
)

enriched = (
    profiled
    .select(
        "id",
        "term",
        "score",
        "grade_status",
        "score_filled",
        "score_distance",
        "enrollment_count",
        "enrollment_term_count",
        "first_enrollment_enrolled_at",
        F.greatest(F.col("enrollment_count"), F.col("score_filled")).alias("greatest_enrollment_count_score_filled"),
    )
)

with_trends = (
    enriched
    .withColumn("last_enrollment_term_count", F.last("enrollment_term_count").over(w_enrollment_count_term_by_sco))
    .withColumn("rows_in_grade_status_enrollment_term_count", F.count(F.lit(1)).over(w_grade_status_enrollment_term))
)

with_trends_2 = (
    with_trends
    .select(
        "id",
        "term",
        "grade_status",
        "score_filled",
        "score_distance",
        "enrollment_count",
        "enrollment_term_count",
        F.count(F.lit(1)).over(w_grade_status_enrollment_coun).alias("rows_in_grade_status_enrollment_count"),
        F.ntile(4).over(w_enrollment_count_last_enroll).alias("first_enrollment_enrolled_at_quartile"),
        F.round(F.avg("score_filled").over(w_enrollment_count_last_enroll_2), 2).alias("score_filled_avg_1_prior"),
        F.round(F.cume_dist().over(w_enrollment_count_last_enroll_3), 3).alias("first_enrollment_enrolled_at_cume_dist"),
    )
)

deduped = (
    with_trends_2
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "term",
        "grade_status",
        "enrollment_term_count",
        "first_enrollment_enrolled_at_quartile",
        "score_filled_avg_1_prior",
        "first_enrollment_enrolled_at_cume_dist",
        F.round(F.col("score_filled") / F.nullif(F.col("score_filled_avg_1_prior"), F.lit(0)), 3).alias("score_filled_per_score_filled_avg_1_prior"),
    )
    .orderBy(F.col("term").desc(), F.col("enrollment_term_count").desc(), "id")
    .limit(5)
)
