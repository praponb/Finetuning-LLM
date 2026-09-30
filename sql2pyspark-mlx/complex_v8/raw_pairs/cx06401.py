from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_student_id_by_enrolled = Window.partitionBy("term", "student_id").orderBy("enrolled_at_dayofweek", "id")
w_enrolled_at_year = Window.partitionBy(F.year("enrolled_at"))

cleaned = (
    df_enrollments
    .filter((F.col("term").isin("2024T1", "2024T2")) & (F.col("score") <= 85.8))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
        F.when(F.col("score") >= 79.2, "large").when(F.col("score") >= 38.5, "medium").otherwise("small").alias("score_tier"),
    )
)

qualified = (
    cleaned
    .crossJoin(cleaned.agg(F.avg("enrolled_at_dayofweek").alias("overall_avg_enrolled_at_dayofweek")))
    .filter(F.col("enrolled_at_dayofweek") > F.col("overall_avg_enrolled_at_dayofweek"))
    .drop("overall_avg_enrolled_at_dayofweek")
)

metrics = (
    qualified
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "term_ref",
        "enrolled_at_dayofweek",
        "score_tier",
        F.row_number().over(w_term_student_id_by_enrolled).alias("rn"),
        F.count(F.lit(1)).over(w_enrolled_at_year).alias("rows_in_enrolled_at_year"),
    )
)

tagged = (
    metrics
    .filter(F.col("rn") >= 5)
    .select(
        "*",
        F.round(F.col("score") * 0.9, 2).alias("score_adj"),
        F.substring(F.trim(F.col("term_ref")), 1, 3).alias("term_ref_prefix"),
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
    )
)

summary = (
    tagged
    .groupBy("enrolled_at_dayofweek")
    .agg(
        F.min("rows_in_enrolled_at_year").alias("min_rows_in_enrolled_at_year"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.min("rn").alias("min_rn"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.count("*").alias("row_count"),
        F.sum("enrolled_at_hour").alias("total_enrolled_at_hour"),
    )
)

result = (
    summary
    .rollup("distinct_student_ids", "enrolled_at_dayofweek", "min_rn")
    .agg(F.round(F.stddev("row_count"), 2).alias("stddev_row_count"))
)
