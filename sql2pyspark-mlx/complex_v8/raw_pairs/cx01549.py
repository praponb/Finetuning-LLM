from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_international = Window.partitionBy("is_international")
w_is_international_major_by_gp = Window.partitionBy("is_international", "major").orderBy(F.col("gpa").desc(), "id")

cleaned = (
    df_students
    .filter((~F.col("is_international")) & (F.upper(F.trim(F.col("student_name"))).like("B%")))
    .select(
        "id",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

screened = (
    cleaned.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.course_id").isNotNull()) & (F.hour(F.col("e.enrolled_at")).between(7, 11)), "left_anti")
)

ranked = (
    screened
    .select(
        "*",
        F.min("gpa").over(w_is_international).alias("is_international_min_gpa"),
        F.count(F.lit(1)).over(w_is_international).alias("rows_in_is_international"),
        F.rank().over(w_is_international_major_by_gp).alias("gpa_rank"),
    )
)

top_rows = ranked.filter((F.col("gpa_rank") <= 5) & (F.col("is_international_min_gpa") < 22.7))

totals = (
    top_rows
    .groupBy("is_international")
    .agg(
        F.round(F.avg("gpa_rank"), 2).alias("avg_gpa_rank"),
        F.round(F.sum("gpa") / F.nullif(F.sum("is_international_min_gpa"), F.lit(0)), 3).alias("gpa_to_is_international_min_gpa_ratio"),
    )
)

annotated = (
    totals
    .select(
        "is_international",
        "avg_gpa_rank",
        "gpa_to_is_international_min_gpa_ratio",
        F.coalesce(F.col("avg_gpa_rank"), F.lit(0.0)).alias("avg_gpa_rank_filled"),
        F.when(F.col("avg_gpa_rank") >= 8.2, "HIGH").when(F.col("avg_gpa_rank") >= 3.2, "MEDIUM").otherwise("LOW").alias("avg_gpa_rank_tier"),
        F.abs(F.col("avg_gpa_rank") - 4.2).alias("avg_gpa_rank_distance"),
    )
)

result = (
    annotated
    .select(
        "is_international",
        "avg_gpa_rank",
        "gpa_to_is_international_min_gpa_ratio",
        "avg_gpa_rank_filled",
        "avg_gpa_rank_tier",
        "avg_gpa_rank_distance",
        (F.floor(F.col("avg_gpa_rank_distance") / 2) * 2).alias("avg_gpa_rank_distance_bucket"),
    )
    .orderBy(F.col("avg_gpa_rank_filled").desc(), "is_international")
    .limit(5)
)
