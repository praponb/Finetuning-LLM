from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("student_id").orderBy(F.col("enrolled_at_month").desc(), F.col("id").desc())
w_latest_2 = Window.partitionBy("student_id", "grade_status").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())

scoped = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
        F.date_trunc("month", F.col("enrolled_at")).alias("enrolled_at_month"),
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
    )
)

screened = (
    scoped.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status").isNotNull()) & (F.col("e.term") != "2024T2"), "left_semi")
    .filter(F.hour(F.col("enrolled_at_month")).between(10, 14))
)

deduped = (
    screened
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

deduped_2 = (
    deduped
    .withColumn("rn", F.row_number().over(w_latest_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

without_matches = (
    deduped_2
    .crossJoin(deduped_2.agg(F.avg("enrolled_at_year").alias("overall_avg_enrolled_at_year")))
    .filter(F.col("enrolled_at_year") > F.col("overall_avg_enrolled_at_year"))
    .drop("overall_avg_enrolled_at_year")
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

with_stats = (
    without_matches.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.avg_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    with_stats
    .filter(F.col("grade_status") == "WITHDRAWN")
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "term_ref",
        "enrolled_at_month",
        "enrollment_count",
        F.lower(F.trim(F.col("term"))).alias("term_clean"),
    )
    .orderBy("term_clean")
)
