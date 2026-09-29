from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("level").orderBy(F.col("credits").desc(), F.col("id").desc())

scoped_courses = (
    df_courses
    .filter((F.col("level").isin("core", "advanced")) & (F.length(F.trim(F.col("course_code"))) >= 5))
    .select(
        "id",
        "dept",
        "credits",
        "level",
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
        F.coalesce(F.col("level"), F.lit("UNKNOWN")).alias("level_or_unknown"),
    )
)

deduped = (
    scoped_courses
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_children = (
    deduped.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
    )
)

enrollment_grade_status_stats = (
    with_children
    .groupBy("enrollment_grade_status")
    .agg(F.avg("enrollment_score").alias("avg_enrollment_score"))
)

above_average = (
    with_children.alias("s")
    .join(enrollment_grade_status_stats.alias("g"), F.col("g.enrollment_grade_status") == F.col("s.enrollment_grade_status"), "inner")
    .filter(F.col("s.enrollment_score") > F.col("g.avg_enrollment_score"))
    .select(
        "s.*",
        "g.avg_enrollment_score",
        F.round(F.col("s.enrollment_score") - F.col("g.avg_enrollment_score"), 2).alias("enrollment_score_minus_avg"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.min("score").alias("min_enrollment_score"))
)

with_counts = (
    above_average.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select("s.*", "e.min_enrollment_score")
)

result = (
    with_counts
    .filter(F.col("enrollment_score_minus_avg").between(-70.0, 30.0))
    .select(
        "id",
        "enrollment_grade_status",
        "avg_enrollment_score",
        "enrollment_score_minus_avg",
        "min_enrollment_score",
        F.when(F.col("avg_enrollment_score") >= 70.0, "HIGH").when(F.col("avg_enrollment_score") >= 30.0, "MEDIUM").otherwise("LOW").alias("avg_enrollment_score_tier"),
    )
    .orderBy("avg_enrollment_score_tier")
)
