from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrollment_count_by_enrollme = Window.partitionBy("enrollment_count").orderBy(F.col("enrollment_count").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrollment_count_by_enrollme_2 = Window.partitionBy("enrollment_count").orderBy(F.col("enrollment_count").desc(), "id")
w_level_len_by_min_enrollment = Window.partitionBy("level_len").orderBy("min_enrollment_count_so_far", "id").rowsBetween(-1, Window.currentRow)
w_level_len_by_min_enrollment_2 = Window.partitionBy("level_len").orderBy("min_enrollment_count_so_far", "id")

base = (
    df_courses
    .filter((F.length(F.trim(F.col("course_code"))) >= 6) & (F.col("credits") <= 3))
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        F.length(F.trim(F.col("level"))).alias("level_len"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.min("score").alias("min_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

profiled = (
    base.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        "e.min_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

enriched = (
    profiled
    .select(
        "id",
        "course_code",
        "level_len",
        "min_enrollment_score",
        "enrollment_count",
        F.substring(F.trim(F.col("dept")), 1, 2).alias("dept_prefix"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.min("enrollment_count").over(w_enrollment_count_by_enrollme).alias("min_enrollment_count_so_far"),
        F.row_number().over(w_enrollment_count_by_enrollme_2).alias("enrollment_count_seq"),
    )
)

annotated = (
    metrics
    .select(
        "id",
        "level_len",
        "dept_prefix",
        "min_enrollment_count_so_far",
        F.least(F.col("level_len"), F.col("enrollment_count")).alias("least_level_len_enrollment_count"),
    )
)

metrics_2 = (
    annotated
    .withColumn("level_len_avg_2", F.round(F.avg("level_len").over(w_level_len_by_min_enrollment), 2))
    .withColumn("min_enrollment_count_so_far_quintile", F.ntile(5).over(w_level_len_by_min_enrollment_2))
)

result = (
    metrics_2
    .select(
        "id",
        "level_len",
        "min_enrollment_count_so_far",
        "level_len_avg_2",
        F.round(F.col("level_len_avg_2") * 1.07, 2).alias("level_len_avg_2_adj"),
        F.substring(F.trim(F.col("dept_prefix")), 1, 3).alias("dept_prefix_prefix"),
    )
    .orderBy(F.col("min_enrollment_count_so_far").desc())
)
