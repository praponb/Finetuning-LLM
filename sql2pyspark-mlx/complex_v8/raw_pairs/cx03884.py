from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrollment_enrolled_a = Window.orderBy("enrollment_enrolled_at", "id", "enrollment_id")
w_level_enrollment_id_by_credi = Window.partitionBy("level", "enrollment_id").orderBy(F.col("credits").desc_nulls_first(), "id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrollment_id_level = Window.partitionBy("enrollment_id", "level")
w_all_by_enrollment_enrolled_a_2 = Window.orderBy("enrollment_enrolled_at", "id", "enrollment_id").rowsBetween(-3, -1)

valid = (
    df_courses
    .filter((F.col("dept") == "CS") & (F.length(F.trim(F.col("course_code"))) >= 8))
    .select(
        "id",
        "dept",
        "credits",
        "level",
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
    )
)

screened = (
    valid.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.student_id") <= 11) & (F.col("e.grade_status").isNotNull()), "left_semi")
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.round(F.avg("score"), 2).alias("avg_enrollment_score_2"),
    )
)

with_counts = (
    screened.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select("s.*", "e.avg_enrollment_score", "e.avg_enrollment_score_2")
)

exploded_detail_enrollments = (
    with_counts.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.enrolled_at") < F.lit("2025-03-08 11:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.credits",
        "s.level",
        "s.avg_enrollment_score",
        "s.avg_enrollment_score_2",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

sequenced = (
    exploded_detail_enrollments
    .select(
        "*",
        F.rank().over(w_all_by_enrollment_enrolled_a).alias("enrollment_enrolled_at_rank"),
        F.nth_value("avg_enrollment_score_2", 2).over(w_level_enrollment_id_by_credi).alias("second_avg_enrollment_score_2"),
        F.round(F.avg("credits").over(w_enrollment_id_level), 2).alias("enrollment_id_level_avg_credits"),
        F.sum("avg_enrollment_score_2").over(w_enrollment_id_level).alias("enrollment_id_level_sum_avg_enrollment_score_2"),
        F.round(F.avg("avg_enrollment_score_2").over(w_all_by_enrollment_enrolled_a_2), 2).alias("avg_enrollment_score_2_avg_3_prior"),
    )
)

enriched = (
    sequenced
    .select(
        "id",
        "credits",
        "level",
        "avg_enrollment_score_2",
        "enrollment_id",
        "enrollment_student_id",
        "enrollment_enrolled_at",
        "enrollment_enrolled_at_rank",
        "enrollment_id_level_avg_credits",
        "avg_enrollment_score_2_avg_3_prior",
        F.abs(F.col("avg_enrollment_score") - 40.0).alias("avg_enrollment_score_distance"),
    )
)

first_rows = enriched.filter(F.col("enrollment_enrolled_at_rank") == 1)

result = (
    first_rows
    .select(
        "id",
        "enrollment_id",
        "enrollment_enrolled_at_rank",
        "credits",
    )
    .unpivot(
        ids=["id", "enrollment_id"],
        values=["enrollment_enrolled_at_rank", "credits"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
