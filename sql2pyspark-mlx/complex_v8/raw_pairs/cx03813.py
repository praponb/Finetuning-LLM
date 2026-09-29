from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("student_id").orderBy("enrolled_at", "id")

in_window = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.score").alias("prior_score_12h"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
    )
)

summarized_rows = (
    in_window.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.min_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

unique_rows = (
    summarized_rows
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

student_id_stats = (
    unique_rows
    .groupBy("student_id")
    .agg(F.avg("enrollment_count_2").alias("avg_enrollment_count_2"))
)

compared = (
    unique_rows.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.enrollment_count_2") > F.col("g.avg_enrollment_count_2"))
    .select(
        "s.*",
        "g.avg_enrollment_count_2",
        F.round(F.col("s.enrollment_count_2") - F.col("g.avg_enrollment_count_2"), 2).alias("enrollment_count_2_minus_avg"),
    )
)

result = (
    compared
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_rows_12h",
        "prior_score_12h",
        "min_enrollment_score",
        "enrollment_count",
        "enrollment_count_2",
        "enrollment_count_2_minus_avg",
        F.coalesce(F.col("min_enrollment_score"), F.lit(0.0)).alias("min_enrollment_score_filled"),
        F.when(F.col("min_enrollment_score") >= 65.0, "large").when(F.col("min_enrollment_score") >= 30.0, "medium").otherwise("small").alias("min_enrollment_score_tier"),
    )
    .orderBy(F.col("min_enrollment_score_filled").desc_nulls_first())
)
