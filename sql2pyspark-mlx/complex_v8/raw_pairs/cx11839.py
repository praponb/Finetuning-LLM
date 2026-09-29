from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("course_id", "term").orderBy(F.col("score").desc(), F.col("id").desc())

filtered_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-18 10:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

qualified = (
    filtered_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.course_code").isNotNull()), "left_semi")
)

latest = (
    qualified
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

annotated = (
    latest
    .select(
        "id",
        "student_id",
        "term",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.abs(F.col("score") - 59.7).alias("score_distance"),
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

result = (
    annotated
    .select(
        "id",
        "term",
        "term_len",
        "score_distance",
        "enrolled_at_quarter",
        (F.col("term_len") - F.col("enrolled_at_quarter")).alias("term_len_minus_enrolled_at_quarter"),
        F.round(F.col("score_distance") * 1.5, 2).alias("score_distance_adj"),
    )
    .orderBy(F.col("score_distance").desc(), "id")
    .limit(5)
)
