from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("level").orderBy(F.col("min_enrollment_score").desc(), F.col("id").desc())

valid_courses = (
    df_courses
    .select(
        "id",
        "course_code",
        "credits",
        "level",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.min("score").alias("min_enrollment_score"))
)

with_stats = (
    valid_courses.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select("s.*", "e.min_enrollment_score")
)

unique_rows = (
    with_stats
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .select(
        "id",
        "course_code",
        "level",
        "min_enrollment_score",
        F.round(F.col("min_enrollment_score") * 0.9, 2).alias("min_enrollment_score_adj"),
    )
    .orderBy("course_code", "id")
    .limit(20)
)
